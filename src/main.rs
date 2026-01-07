use anyhow::{Context, Result};
use crossbeam_channel::bounded;
use dashmap::DashMap;
use io_splicer_demo::{InputSource, IoSplicer, SplicerConfig, SplicerStats};
use regex::Regex;
use std::env;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    // 1. Parse CLI Arguments
    // Usage: cargo run -- [directory] [regex_filter]
    let args: Vec<String> = env::args().collect();
    
    let target_dir = args.get(1).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    
    // Optional Regex filter from 2nd argument
    let filter_pattern = args.get(2).cloned();
    let path_filter = if let Some(pattern) = filter_pattern {
        println!("Filtering files matching: {}", pattern);
        Some(Regex::new(&pattern).context("Invalid regex provided")?)
    } else {
        None
    };

    // 2. Setup Configuration
    let config = SplicerConfig {
        chunk_size: 256 * 1024,       // 256KB chunks
        max_buffer_size: 1024 * 1024, // 1MB buffer
        path_filter,
        recursive: true,
    };

    // 3. Setup Concurrent Map (Java-like ConcurrentHashMap)
    // DashMap uses sharding to allow high concurrency without a global lock.
    let word_counts = Arc::new(DashMap::new());

    // 4. Setup Stats and Channels
    let stats = Arc::new(SplicerStats::default());
    let (tx, rx) = bounded::<String>(100);

    // 5. Start Ticker Thread
    let stats_monitor = stats.clone();
    thread::spawn(move || {
        let start = std::time::Instant::now();
        loop {
            thread::sleep(Duration::from_secs(1));
            let elapsed = start.elapsed().as_secs_f64();
            let files = stats_monitor.file_count.load(Ordering::Relaxed);
            let bytes = stats_monitor.byte_count.load(Ordering::Relaxed);
            let chunks = stats_monitor.chunk_count.load(Ordering::Relaxed);
            
            let mb = bytes as f64 / 1024.0 / 1024.0;
            println!(
                "Processing: [Files: {}] [Chunks: {}] [Bytes: {:.2} MB] [Avg Rate: {:.2} MB/s]", 
                files, chunks, mb, mb / elapsed
            );
        }
    });

    // 6. Start Worker Threads (Word Counters)
    let num_workers = 4;
    let mut handles = vec![];
    
    for _ in 0..num_workers {
        let rx_worker = rx.clone();
        let map_worker = word_counts.clone();
        
        handles.push(thread::spawn(move || {
            while let Ok(chunk) = rx_worker.recv() {
                // Split by whitespace and count
                for word in chunk.split_whitespace() {
                    // Normalize to lowercase to avoid "The" vs "the"
                    let w = word.to_lowercase();
                    
                    // Atomic update in the map
                    *map_worker.entry(w).or_insert(0) += 1;
                }
            }
        }));
    }

    // 7. Run the Splicer
    println!("Starting scan on {:?}...", target_dir);
    let splicer = IoSplicer::new(config, stats.clone(), tx);
    
    splicer.run(InputSource::Directory(target_dir))?;

    // Drop splicer to close the sender channel, signaling workers to finish
    drop(splicer);

    // Wait for workers to finish counting
    for h in handles {
        h.join().unwrap();
    }

    // 8. Aggregate and Sort Results
    println!("\n--- Processing Complete ---");
    println!("Total Unique Words: {}", word_counts.len());
    println!("Top 20 Words:");

    // Convert DashMap to Vec to sort
    let mut count_vec: Vec<(String, usize)> = word_counts
        .iter()
        .map(|entry| (entry.key().clone(), *entry.value()))
        .collect();

    // Sort descending by count
    count_vec.sort_by(|a, b| b.1.cmp(&a.1));

    for (i, (word, count)) in count_vec.iter().take(20).enumerate() {
        println!("{}. '{}': {}", i + 1, word, count);
    }

    Ok(())
}