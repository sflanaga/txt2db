use anyhow::{Context, Result};
use bzip2::read::BzDecoder;
use crossbeam_channel::{bounded, Receiver, Sender};
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
    pub paths_queued: AtomicIsize,
    
    pub recycler_miss_count: AtomicUsize,
    pub buffer_realloc_count: AtomicUsize,
    pub newline_split_count: AtomicUsize,
    pub forced_split_count: AtomicUsize,
}

/// The payload sent to workers. Contains the raw data and metadata.
pub struct SplicedChunk {
    pub data: Vec<u8>,
    pub file_path: Option<Arc<PathBuf>>,
    /// Byte offset where this chunk starts in the file
    pub offset: u64,
}

pub enum InputSource {
    Stdin,
    FileList(Vec<PathBuf>),
    Directory(PathBuf),
}

pub struct IoSplicer {
    config: SplicerConfig,
    stats: Arc<SplicerStats>,
    sender: Sender<SplicedChunk>,
    recycler: Receiver<Vec<u8>>,
}

impl IoSplicer {
    pub fn new(
        config: SplicerConfig,
        stats: Arc<SplicerStats>,
        sender: Sender<SplicedChunk>,
        recycler: Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            config,
            stats,
            sender,
            recycler,
        }
    }

    pub fn run(&self, input: InputSource) -> Result<()> {
        match input {
            InputSource::Stdin => {
                let stdin = io::stdin();
                self.process_reader(stdin.lock(), None)?;
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
        let (path_tx, path_rx) = bounded::<PathBuf>(1024);
        let num_threads = if self.config.thread_count > 0 { self.config.thread_count } else { 4 };

        thread::scope(|s| {
            s.spawn(move || {
                for path in path_iter {
                    self.stats.paths_queued.fetch_add(1, Ordering::Relaxed);
                    if path_tx.send(path).is_err() { break; }
                }
            });

            for _ in 0..num_threads {
                let rx = path_rx.clone();
                s.spawn(move || {
                    while let Ok(path) = rx.recv() {
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
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };
        
        self.stats.file_count.fetch_add(1, Ordering::Relaxed);
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        
        // Wrap path in Arc for cheap sharing with chunks
        let path_arc = Some(Arc::new(path.to_path_buf()));

        match ext {
            "gz" => self.process_reader(BufReader::new(GzDecoder::new(file)), path_arc),
            "bz2" => self.process_reader(BufReader::new(BzDecoder::new(file)), path_arc),
            "zst" => self.process_reader(BufReader::new(ZstdDecoder::new(file)?), path_arc),
            _ => self.process_reader(BufReader::new(file), path_arc),
        }
    }

    fn process_reader<R: Read>(&self, mut reader: R, path: Option<Arc<PathBuf>>) -> Result<()> {
        let mut buffer = Vec::with_capacity(self.config.max_buffer_size);
        let mut read_buf = vec![0u8; 128 * 1024]; 
        let mut current_offset: u64 = 0;

        loop {
            let bytes_read = reader.read(&mut read_buf)?;
            if bytes_read == 0 {
                break;
            }

            self.stats.byte_count.fetch_add(bytes_read, Ordering::Relaxed);
            buffer.extend_from_slice(&read_buf[..bytes_read]);

            while buffer.len() >= self.config.chunk_size {
                let search_limit = std::cmp::min(buffer.len(), self.config.max_buffer_size);
                let slice_to_check = &buffer[..search_limit];
                
                if let Some(last_newline_idx) = slice_to_check.iter().rposition(|&b| b == b'\n') {
                    self.stats.newline_split_count.fetch_add(1, Ordering::Relaxed);
                    
                    let split_len = last_newline_idx + 1;
                    self.emit_chunk(&buffer[..split_len], path.clone(), current_offset)?;
                    
                    buffer.drain(..split_len);
                    current_offset += split_len as u64;
                } else {
                    if buffer.len() >= self.config.max_buffer_size {
                        self.stats.forced_split_count.fetch_add(1, Ordering::Relaxed);
                        
                        self.emit_chunk(&buffer[..self.config.max_buffer_size], path.clone(), current_offset)?;
                        
                        buffer.drain(..self.config.max_buffer_size);
                        current_offset += self.config.max_buffer_size as u64;
                    } else {
                        break;
                    }
                }
            }
        }

        if !buffer.is_empty() {
            self.emit_chunk(&buffer, path, current_offset)?;
        }

        Ok(())
    }

    fn emit_chunk(&self, bytes: &[u8], path: Option<Arc<PathBuf>>, offset: u64) -> Result<()> {
        let mut v = match self.recycler.try_recv() {
            Ok(mut b) => {
                b.clear();
                b
            },
            Err(_) => {
                self.stats.recycler_miss_count.fetch_add(1, Ordering::Relaxed);
                Vec::with_capacity(bytes.len())
            }
        };

        if v.capacity() < bytes.len() {
            self.stats.buffer_realloc_count.fetch_add(1, Ordering::Relaxed);
        }

        v.extend_from_slice(bytes);
        
        let chunk = SplicedChunk {
            data: v,
            file_path: path,
            offset,
        };
        
        self.sender.send(chunk).context("Receiver dropped")?;
        self.stats.chunk_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}