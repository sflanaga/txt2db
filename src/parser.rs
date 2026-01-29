use crossbeam_channel::{Receiver, Sender};
use pcre2::bytes::Regex as PcreRegex;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::aggregation::{AggAccumulator, AggRole, AggValue, AggFieldSource, MapFieldSpec};
use crate::config::{DisableConfig, FieldSource as ConfigFieldSource};
use crate::database::{ColumnDef, DbRecord, TypedValue};
use crate::io_splicer::SplicedChunk;
use crate::stats::DbStats;

#[derive(Clone)]
pub enum AnyRegex {
    Std(Regex),
    Pcre(PcreRegex),
}

// Normalize paths so user-supplied regexes that assume '/' work on platforms that
// emit backslashes (e.g., Windows).
fn normalize_path_for_regex(path: &std::path::Path) -> String {
    let raw = path.to_string_lossy();
    if raw.contains('\\') {
        raw.replace('\\', "/")
    } else {
        raw.into_owned()
    }
}

impl AnyRegex {
    pub fn captures_len(&self) -> usize {
        match self {
            AnyRegex::Std(r) => r.captures_len(),
            // PCRE2 captures_len() returns the number of capturing groups (excluding group 0).
            // Rust regex captures_len() includes group 0.
            // We add 1 to PCRE2 to match the behavior.
            AnyRegex::Pcre(r) => r.captures_len() + 1,
        }
    }
}

fn fetch_path_caps<'a>(
    path_cache: &'a mut HashMap<Arc<std::path::PathBuf>, Option<Vec<Option<String>>>>,
    path_re: &Regex,
    path_arc: &Arc<std::path::PathBuf>,
) -> Option<&'a Option<Vec<Option<String>>>> {
    if !path_cache.contains_key(path_arc) {
        let p_norm = normalize_path_for_regex(path_arc.as_path());
        let entry = if let Some(caps) = path_re.captures(&p_norm) {
            let mut v = Vec::with_capacity(path_re.captures_len());
            for i in 0..path_re.captures_len() {
                v.push(caps.get(i).map(|m| m.as_str().to_string()));
            }
            Some(v)
        } else {
            None
        };
        path_cache.insert(path_arc.clone(), entry);
    }
    path_cache.get(path_arc)
}

pub fn run_mapper_worker(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    specs: Vec<MapFieldSpec>,
    line_re: AnyRegex,
    path_re: Option<Regex>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
    disable_flags: Arc<DisableConfig>,
) -> BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> {
    match line_re {
        AnyRegex::Std(re) => run_mapper_worker_std(
            rx,
            recycle_tx,
            specs,
            re,
            path_re,
            stats,
            show_errors,
            stop_on_error,
            disable_flags,
        ),
        AnyRegex::Pcre(re) => run_mapper_worker_pcre(
            rx,
            recycle_tx,
            specs,
            re,
            path_re,
            stats,
            show_errors,
            stop_on_error,
            disable_flags,
        ),
    }
}

fn run_mapper_worker_std(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    specs: Vec<MapFieldSpec>,
    line_re: Regex,
    path_re: Option<Regex>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
    disable_flags: Arc<DisableConfig>,
) -> BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> {
    let mut map: BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> = BTreeMap::new();
    let mut path_cache: HashMap<Arc<std::path::PathBuf>, Option<Vec<Option<String>>>> =
        HashMap::new();

    // Pre-calculate indices
    let mut key_indices = Vec::new();
    let mut val_indices = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        if spec.role == AggRole::Key {
            key_indices.push(i);
        } else {
            val_indices.push(i);
        }
    }

    while let Ok(chunk) = rx.recv() {
        let mut data = chunk.data;
        stats
            .bytes_processed
            .fetch_add(data.len(), Ordering::Relaxed);

        if disable_flags.regex {
            data.clear();
            let _ = recycle_tx.send(data);
            continue;
        }

        // Path captures (once per file path, cached)
        let path_caps = if let (Some(pr), Some(ref path_arc)) = (&path_re, chunk.file_path.as_ref())
        {
            match fetch_path_caps(&mut path_cache, pr, path_arc) {
                Some(Some(v)) => Some(v),
                Some(None) => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                }
                None => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                }
            }
        } else {
            None
        };

        let path_display = chunk
            .file_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "stdin".to_string());
        let chunk_offset = chunk.offset;

        // STD Regex works on UTF-8 strings
        let s = String::from_utf8_lossy(&data);

        for capture in line_re.captures_iter(&s) {
            stats.matched_lines.fetch_add(1, Ordering::Relaxed);

            if disable_flags.map_target {
                continue;
            }

            let mut agg_row = Vec::with_capacity(specs.len());
            let mut row_has_error = false;

            let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
            let abs_offset = chunk_offset + match_start as u64;

            for spec in &specs {
                let raw = match spec.source {
                    AggFieldSource::Line => capture
                        .get(spec.capture_index)
                        .map(|m| m.as_str())
                        .unwrap_or(""),
                    AggFieldSource::Path => path_caps
                        .and_then(|caps| {
                            caps.get(spec.capture_index).and_then(|o| o.as_deref())
                        })
                        .unwrap_or(""),
                };
                match AggValue::from_str(raw, spec.dtype) {
                    Some(val) => agg_row.push(val),
                    None => {
                        row_has_error = true;
                        stats.total_errors.fetch_add(1, Ordering::Relaxed);
                        *stats.error_counts.entry(spec.capture_index).or_default() += 1;

                        if show_errors || stop_on_error {
                            eprintln!("Parse Error at {}:{} (Capture Group {}): Failed to parse '{}' as {:?}", 
                                path_display, abs_offset, spec.capture_index, raw, spec.dtype);
                        }
                        if stop_on_error {
                            eprintln!("Stopping due to error (-E flag).");
                            std::process::exit(1);
                        }
                    }
                }
            }

            if row_has_error {
                continue;
            }

            if disable_flags.map_write {
                continue;
            }

            let mut key = Vec::with_capacity(key_indices.len());
            let mut key_valid = true;
            for &idx in &key_indices {
                let v = &agg_row[idx];
                if v.is_null() {
                    key_valid = false;
                    break;
                }
                key.push(v.clone());
            }

            if key_valid {
                stats.mapped_records.fetch_add(1, Ordering::Relaxed);
                let entry = map.entry(key).or_insert_with(|| {
                    val_indices
                        .iter()
                        .map(|&i| AggAccumulator::new(specs[i].role, specs[i].dtype))
                        .collect()
                });
                for (acc_idx, &row_idx) in val_indices.iter().enumerate() {
                    entry[acc_idx].update(&agg_row[row_idx]);
                }
            }
        }

        data.clear();
        let _ = recycle_tx.send(data);
    }

    map
}

fn run_mapper_worker_pcre(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    specs: Vec<MapFieldSpec>,
    line_re: PcreRegex,
    path_re: Option<Regex>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
    disable_flags: Arc<DisableConfig>,
) -> BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> {
    let mut map: BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> = BTreeMap::new();
    let mut path_cache: HashMap<Arc<std::path::PathBuf>, Option<Vec<Option<String>>>> =
        HashMap::new();

    let mut key_indices = Vec::new();
    let mut val_indices = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        if spec.role == AggRole::Key {
            key_indices.push(i);
        } else {
            val_indices.push(i);
        }
    }

    while let Ok(chunk) = rx.recv() {
        let mut data = chunk.data;
        stats
            .bytes_processed
            .fetch_add(data.len(), Ordering::Relaxed);

        if disable_flags.regex {
            data.clear();
            let _ = recycle_tx.send(data);
            continue;
        }

        let path_caps = if let (Some(pr), Some(ref path_arc)) = (&path_re, chunk.file_path.as_ref())
        {
            match fetch_path_caps(&mut path_cache, pr, path_arc) {
                Some(Some(v)) => Some(v),
                Some(None) => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                }
                None => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                }
            }
        } else {
            None
        };

        let path_display = chunk
            .file_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "stdin".to_string());
        let chunk_offset = chunk.offset;

        // PCRE2 works on bytes directly
        for result in line_re.captures_iter(&data) {
            if let Ok(capture) = result {
                stats.matched_lines.fetch_add(1, Ordering::Relaxed);

                if disable_flags.map_target {
                    continue;
                }

                let mut agg_row = Vec::with_capacity(specs.len());
                let mut row_has_error = false;

                let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                let abs_offset = chunk_offset + match_start as u64;

                for spec in &specs {
                    let raw = match spec.source {
                        AggFieldSource::Line => {
                            let raw_bytes = capture
                                .get(spec.capture_index)
                                .map(|m| m.as_bytes())
                                .unwrap_or(&[]);
                            String::from_utf8_lossy(raw_bytes)
                        }
                        AggFieldSource::Path => path_caps
                            .and_then(|caps| {
                                caps.get(spec.capture_index).and_then(|o| o.as_deref())
                            })
                            .map(|s| std::borrow::Cow::Borrowed(s))
                            .unwrap_or_else(|| std::borrow::Cow::Borrowed("")),
                    };

                    match AggValue::from_str(&raw, spec.dtype) {
                        Some(val) => agg_row.push(val),
                        None => {
                            row_has_error = true;
                            stats.total_errors.fetch_add(1, Ordering::Relaxed);
                            *stats.error_counts.entry(spec.capture_index).or_default() += 1;

                            if show_errors || stop_on_error {
                                eprintln!("Parse Error at {}:{} (Capture Group {}): Failed to parse '{}' as {:?}", 
                                    path_display, abs_offset, spec.capture_index, raw, spec.dtype);
                            }
                            if stop_on_error {
                                eprintln!("Stopping due to error (-E flag).");
                                std::process::exit(1);
                            }
                        }
                    }
                }

                if row_has_error {
                    continue;
                }
                if disable_flags.map_write {
                    continue;
                }

                let mut key = Vec::with_capacity(key_indices.len());
                let mut key_valid = true;
                for &idx in &key_indices {
                    let v = &agg_row[idx];
                    if v.is_null() {
                        key_valid = false;
                        break;
                    }
                    key.push(v.clone());
                }

                if key_valid {
                    stats.mapped_records.fetch_add(1, Ordering::Relaxed);
                    let entry = map.entry(key).or_insert_with(|| {
                        val_indices
                            .iter()
                            .map(|&i| AggAccumulator::new(specs[i].role, specs[i].dtype))
                            .collect()
                    });
                    for (acc_idx, &row_idx) in val_indices.iter().enumerate() {
                        entry[acc_idx].update(&agg_row[row_idx]);
                    }
                }
            }
        }

        data.clear();
        let _ = recycle_tx.send(data);
    }

    map
}

pub fn run_db_parser(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    db_tx: Sender<DbRecord>,
    line_re: AnyRegex,
    path_re: Option<Regex>,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
) {
    match line_re {
        AnyRegex::Std(re) => run_db_parser_std(rx, recycle_tx, db_tx, re, path_re, columns, stats, show_errors, stop_on_error),
        AnyRegex::Pcre(re) => {
            run_db_parser_pcre(rx, recycle_tx, db_tx, re, path_re, columns, stats, show_errors, stop_on_error)
        }
    }
}

fn run_db_parser_std(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    db_tx: Sender<DbRecord>,
    line_re: Regex,
    path_re: Option<Regex>,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
) {
    let mut should_stop = false;

    while let Ok(chunk) = rx.recv() {
        if should_stop {
            let mut data = chunk.data;
            data.clear();
            let _ = recycle_tx.send(data);
            continue;
        }

        let mut data = chunk.data;
        stats
            .bytes_processed
            .fetch_add(data.len(), Ordering::Relaxed);

        let path_arc = chunk.file_path;
        let chunk_offset = chunk.offset;

        let mut should_process = true;
        let mut path_fields: Vec<(usize, String)> = Vec::with_capacity(columns.len());

        if let Some(pre) = &path_re {
            if let Some(p) = &path_arc {
                let p_norm = normalize_path_for_regex(p.as_path());
                if let Some(caps) = pre.captures(&p_norm) {
                    for col in &columns {
                        if let ConfigFieldSource::Path(idx) = col.source {
                            let val = caps
                                .get(idx)
                                .map(|m| m.as_str().to_string())
                                .unwrap_or_default();
                            path_fields.push((idx, val));
                        }
                    }
                } else {
                    should_process = false;
                }
            } else {
                should_process = false;
            }
        }

        if should_process {
            let s = String::from_utf8_lossy(&data);

            let path_display = path_arc
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "stdin".to_string());

            for capture in line_re.captures_iter(&s) {
                stats.matched_lines.fetch_add(1, Ordering::Relaxed);

                // Extract Full Line Logic
                let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                let match_end = capture.get(0).map(|m| m.end()).unwrap_or(0);
                let bytes = s.as_bytes();

                let start_idx = bytes[..match_start]
                    .iter()
                    .rposition(|&b| b == b'\n')
                    .map(|i| i + 1)
                    .unwrap_or(0);

                let end_idx = bytes[match_end..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|i| match_end + i)
                    .unwrap_or(bytes.len());

                let full_line = s[start_idx..end_idx].trim_end().to_string();
                let match_offset = chunk_offset + start_idx as u64;

                let mut fields = Vec::with_capacity(columns.len());
                let mut row_has_error = false;

                for col in &columns {
                    let raw_str = match &col.source {
                        ConfigFieldSource::Line(idx) => {
                            capture.get(*idx).map(|m| m.as_str()).unwrap_or("")
                        }
                        ConfigFieldSource::Path(idx) => {
                            path_fields
                                .iter()
                                .find(|(k, _)| k == idx)
                                .map(|(_, v)| v.as_str())
                                .unwrap_or("")
                        }
                        ConfigFieldSource::None => "",
                    };

                    match TypedValue::parse(raw_str, col.dtype) {
                        Ok(val) => fields.push(val),
                        Err(e) => {
                            row_has_error = true;
                            stats.total_errors.fetch_add(1, Ordering::Relaxed);
                            if show_errors {
                                eprintln!(
                                    "Type error: {} at {}:{} field '{}'",
                                    e, path_display, match_offset, col.name
                                );
                            }
                            if stop_on_error {
                                should_stop = true;
                                break;
                            }
                            // Insert NULL on error and continue
                            fields.push(TypedValue::Null);
                        }
                    }
                }

                if should_stop {
                    break;
                }

                // Skip row if it had errors (already counted)
                if row_has_error {
                    continue;
                }

                let record = DbRecord::Data {
                    file_path: path_arc.clone(),
                    offset: match_offset,
                    line_content: full_line,
                    fields,
                };
                if db_tx.send(record).is_err() {
                    break;
                }
            }
        }
        data.clear();
        let _ = recycle_tx.send(data);
    }
}

fn run_db_parser_pcre(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    db_tx: Sender<DbRecord>,
    line_re: PcreRegex,
    path_re: Option<Regex>,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
) {
    let mut should_stop = false;

    while let Ok(chunk) = rx.recv() {
        if should_stop {
            let mut data = chunk.data;
            data.clear();
            let _ = recycle_tx.send(data);
            continue;
        }

        let mut data = chunk.data;
        stats
            .bytes_processed
            .fetch_add(data.len(), Ordering::Relaxed);

        let path_arc = chunk.file_path;
        let chunk_offset = chunk.offset;

        let mut should_process = true;
        let mut path_fields: Vec<(usize, String)> = Vec::with_capacity(columns.len());

        if let Some(pre) = &path_re {
            if let Some(p) = &path_arc {
                let p_norm = normalize_path_for_regex(p.as_path());
                if let Some(caps) = pre.captures(&p_norm) {
                    for col in &columns {
                        if let ConfigFieldSource::Path(idx) = col.source {
                            let val = caps
                                .get(idx)
                                .map(|m| m.as_str().to_string())
                                .unwrap_or_default();
                            path_fields.push((idx, val));
                        }
                    }
                } else {
                    should_process = false;
                }
            } else {
                should_process = false;
            }
        }

        if should_process {
            let path_display = path_arc
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "stdin".to_string());

            // PCRE2 Iteration
            for result in line_re.captures_iter(&data) {
                if let Ok(capture) = result {
                    stats.matched_lines.fetch_add(1, Ordering::Relaxed);

                    let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                    let match_end = capture.get(0).map(|m| m.end()).unwrap_or(0);
                    let bytes = &data;

                    let start_idx = bytes[..match_start]
                        .iter()
                        .rposition(|&b| b == b'\n')
                        .map(|i| i + 1)
                        .unwrap_or(0);

                    let end_idx = bytes[match_end..]
                        .iter()
                        .position(|&b| b == b'\n')
                        .map(|i| match_end + i)
                        .unwrap_or(bytes.len());

                    let full_line_bytes = &bytes[start_idx..end_idx];
                    let full_line = String::from_utf8_lossy(full_line_bytes)
                        .trim_end()
                        .to_string();
                    let match_offset = chunk_offset + start_idx as u64;

                    let mut fields = Vec::with_capacity(columns.len());
                    let mut row_has_error = false;

                    for col in &columns {
                        let raw_str = match &col.source {
                            ConfigFieldSource::Line(idx) => {
                                capture
                                    .get(*idx)
                                    .map(|m| String::from_utf8_lossy(m.as_bytes()).to_string())
                                    .unwrap_or_default()
                            }
                            ConfigFieldSource::Path(idx) => {
                                path_fields
                                    .iter()
                                    .find(|(k, _)| k == idx)
                                    .map(|(_, v)| v.clone())
                                    .unwrap_or_default()
                            }
                            ConfigFieldSource::None => String::new(),
                        };

                        match TypedValue::parse(&raw_str, col.dtype) {
                            Ok(val) => fields.push(val),
                            Err(e) => {
                                row_has_error = true;
                                stats.total_errors.fetch_add(1, Ordering::Relaxed);
                                if show_errors {
                                    eprintln!(
                                        "Type error: {} at {}:{} field '{}'",
                                        e, path_display, match_offset, col.name
                                    );
                                }
                                if stop_on_error {
                                    should_stop = true;
                                    break;
                                }
                                // Insert NULL on error and continue
                                fields.push(TypedValue::Null);
                            }
                        }
                    }

                    if should_stop {
                        break;
                    }

                    // Skip row if it had errors
                    if row_has_error {
                        continue;
                    }

                    let record = DbRecord::Data {
                        file_path: path_arc.clone(),
                        offset: match_offset,
                        line_content: full_line,
                        fields,
                    };
                    if db_tx.send(record).is_err() {
                        break;
                    }
                }
            }
        }
        data.clear();
        let _ = recycle_tx.send(data);
    }
}
