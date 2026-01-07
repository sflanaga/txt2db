use anyhow::{Context, Result};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use regex::Regex;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use walkdir::WalkDir;
use zstd::stream::read::Decoder as ZstdDecoder;

#[derive(Clone, Debug)]
pub struct SplicerConfig {
    pub chunk_size: usize,
    pub max_buffer_size: usize,
    pub path_filter: Option<Regex>,
    pub recursive: bool,
}

impl Default for SplicerConfig {
    fn default() -> Self {
        Self {
            chunk_size: 256 * 1024,
            max_buffer_size: 2 * 1024 * 1024,
            path_filter: None,
            recursive: true,
        }
    }
}

#[derive(Debug, Default)]
pub struct SplicerStats {
    pub file_count: AtomicUsize,
    pub byte_count: AtomicUsize,
    pub chunk_count: AtomicUsize,
}

pub enum InputSource {
    Stdin,
    FileList(Vec<PathBuf>),
    Directory(PathBuf),
}

pub struct IoSplicer {
    config: SplicerConfig,
    stats: Arc<SplicerStats>,
    sender: crossbeam_channel::Sender<String>,
}

impl IoSplicer {
    pub fn new(
        config: SplicerConfig,
        stats: Arc<SplicerStats>,
        sender: crossbeam_channel::Sender<String>,
    ) -> Self {
        Self {
            config,
            stats,
            sender,
        }
    }

    pub fn run(&self, input: InputSource) -> Result<()> {
        match input {
            InputSource::Stdin => {
                let stdin = io::stdin();
                self.process_reader(stdin.lock())?;
            }
            InputSource::FileList(paths) => {
                for path in paths {
                    self.process_file(&path)?;
                }
            }
            InputSource::Directory(root) => {
                let mut walker = WalkDir::new(&root);
                if !self.config.recursive {
                    walker = walker.max_depth(1);
                }

                // Note: In a production app, you might parallelize the file walking/opening here too
                // using something like `par_iter` from Rayon or a thread pool, but we keep it serial
                // here to ensure the Splicer feeds the worker pool in a defined order.
                for entry in walker.into_iter().filter_map(|e| e.ok()) {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(re) = &self.config.path_filter {
                            if let Some(path_str) = path.to_str() {
                                if !re.is_match(path_str) {
                                    continue;
                                }
                            }
                        }
                        self.process_file(path)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn process_file(&self, path: &Path) -> Result<()> {
        let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
        self.stats.file_count.fetch_add(1, Ordering::Relaxed);

        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");

        // Transparent Decompression
        match ext {
            "gz" => self.process_reader(BufReader::new(GzDecoder::new(file))),
            "bz2" => self.process_reader(BufReader::new(BzDecoder::new(file))),
            "zst" => self.process_reader(BufReader::new(ZstdDecoder::new(file)?)),
            _ => self.process_reader(BufReader::new(file)),
        }
    }

    fn process_reader<R: Read>(&self, mut reader: R) -> Result<()> {
        let mut buffer = Vec::with_capacity(self.config.max_buffer_size);
        let mut read_buf = vec![0u8; 64 * 1024];

        loop {
            let bytes_read = reader.read(&mut read_buf)?;
            if bytes_read == 0 {
                break;
            }

            self.stats.byte_count.fetch_add(bytes_read, Ordering::Relaxed);
            buffer.extend_from_slice(&read_buf[..bytes_read]);

            while buffer.len() >= self.config.chunk_size {
                let window_limit = std::cmp::min(buffer.len(), self.config.max_buffer_size);
                let window = &buffer[..window_limit];

                if let Some(last_newline_pos) = window.iter().rposition(|&b| b == b'\n') {
                    let split_idx = last_newline_pos + 1;
                    self.emit_chunk(&buffer[..split_idx])?;
                    buffer.drain(..split_idx);
                } else {
                    if buffer.len() >= self.config.max_buffer_size {
                        self.emit_chunk(&buffer[..self.config.max_buffer_size])?;
                        buffer.drain(..self.config.max_buffer_size);
                    } else {
                        break;
                    }
                }
            }
        }

        if !buffer.is_empty() {
            self.emit_chunk(&buffer)?;
        }

        Ok(())
    }

    fn emit_chunk(&self, bytes: &[u8]) -> Result<()> {
        let chunk_str = String::from_utf8_lossy(bytes).into_owned();
        self.sender.send(chunk_str).context("Receiver dropped")?;
        self.stats.chunk_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}