use anyhow::{Context, Result};
use bzip2::read::BzDecoder;
use crossbeam_channel::{bounded, Sender};
use flate2::read::GzDecoder;
use regex::Regex;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use walkdir::WalkDir;
use zstd::stream::read::Decoder as ZstdDecoder;

#[derive(Clone, Debug)]
pub struct SplicerConfig {
    pub chunk_size: usize,
    pub max_buffer_size: usize,
    pub path_filter: Option<Regex>,
    pub recursive: bool,
    /// Number of threads to use for file reading/splicing.
    /// If 0, defaults to 4.
    pub thread_count: usize,
}

impl Default for SplicerConfig {
    fn default() -> Self {
        Self {
            chunk_size: 256 * 1024,
            max_buffer_size: 2 * 1024 * 1024,
            path_filter: None,
            recursive: true,
            thread_count: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct SplicerStats {
    pub file_count: AtomicUsize,
    pub byte_count: AtomicUsize,
    pub chunk_count: AtomicUsize,
    // Tracks how many files are sitting in the internal channel waiting to be processed
    pub paths_queued: AtomicIsize,
}

pub enum InputSource {
    Stdin,
    FileList(Vec<PathBuf>),
    Directory(PathBuf),
}

pub struct IoSplicer {
    config: SplicerConfig,
    stats: Arc<SplicerStats>,
    sender: Sender<String>,
}

impl IoSplicer {
    pub fn new(
        config: SplicerConfig,
        stats: Arc<SplicerStats>,
        sender: Sender<String>,
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
                self.run_parallel_splicers(paths.into_iter())?;
            }
            InputSource::Directory(root) => {
                let config = self.config.clone();
                let walker = WalkDir::new(&root).max_depth(if config.recursive { usize::MAX } else { 1 });

                let path_iter = walker
                    .into_iter()
                    .filter_map(move |e| e.ok())
                    .filter(move |e| {
                        if !e.file_type().is_file() {
                            return false;
                        }
                        if let Some(re) = &config.path_filter {
                            if let Some(s) = e.path().to_str() {
                                if !re.is_match(s) {
                                    return false;
                                }
                            }
                        }
                        true
                    })
                    .map(|e| e.path().to_path_buf());

                self.run_parallel_splicers(path_iter)?;
            }
        }
        Ok(())
    }

    fn run_parallel_splicers<I>(&self, path_iter: I) -> Result<()>
    where
        I: Iterator<Item = PathBuf> + Send,
    {
        // 1. Channel to distribute file paths to splicer threads
        let (path_tx, path_rx) = bounded::<PathBuf>(1024);

        let num_threads = if self.config.thread_count > 0 {
            self.config.thread_count
        } else {
            4
        };

        // 2. Orchestrate threads
        thread::scope(|s| {
            // A. Walker Thread: Pushes paths into queue
            s.spawn(move || {
                for path in path_iter {
                    self.stats.paths_queued.fetch_add(1, Ordering::Relaxed);
                    if path_tx.send(path).is_err() {
                        break;
                    }
                }
            });

            // B. Splicer Threads: Pull paths, read file, push chunks
            for _ in 0..num_threads {
                let rx = path_rx.clone();
                s.spawn(move || {
                    while let Ok(path) = rx.recv() {
                        // We popped a file from queue, so decrement stat
                        self.stats.paths_queued.fetch_sub(1, Ordering::Relaxed);

                        if let Err(e) = self.process_file(&path) {
                            eprintln!("Error processing {:?}: {}", path, e);
                        }
                    }
                });
            }
        });

        Ok(())
    }

    fn process_file(&self, path: &Path) -> Result<()> {
        // Silently ignore open errors (permissions etc) to keep moving
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