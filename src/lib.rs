use anyhow::{Context, Result};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use rayon::prelude::*;
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
                // Process the provided list in parallel
                paths.par_iter().try_for_each(|path| self.process_file(path))?;
            }
            InputSource::Directory(root) => {
                let mut walker = WalkDir::new(&root);
                if !self.config.recursive {
                    walker = walker.max_depth(1);
                }

                // FIX: Use par_bridge() to stream files immediately into the thread pool.
                // This prevents waiting for the entire directory walk to finish before processing starts.
                walker.into_iter()
                    .par_bridge() 
                    .try_for_each(|entry_res| -> Result<()> {
                        // If we hit a permission error on a file, we might want to log and continue, 
                        // but here we follow typical strict behavior or ignore access errors.
                        if let Ok(entry) = entry_res {
                            let path = entry.path();
                            if path.is_file() {
                                // Check regex filter
                                let include = if let Some(re) = &self.config.path_filter {
                                    path.to_str().map(|s| re.is_match(s)).unwrap_or(false)
                                } else {
                                    true
                                };

                                if include {
                                    // Process file in this rayon thread
                                    self.process_file(path)?;
                                }
                            }
                        }
                        Ok(())
                    })?;
            }
        }
        Ok(())
    }

    fn process_file(&self, path: &Path) -> Result<()> {
        // We ignore file open errors (like permission denied) to keep the batch moving
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(()), 
        };
        
        self.stats.file_count.fetch_add(1, Ordering::Relaxed);

        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");

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