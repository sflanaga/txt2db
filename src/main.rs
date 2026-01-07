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
    let args: Vec<String> = env::args().collect();
    
    // Default to current dir if not provided
    let target_dir = args.get(1).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    
    // Optional Regex filter
    let filter_pattern = args.get(2).cloned();
    let path_filter = if let Some(pattern) = filter_pattern {
        println!("Filter Pattern:   {}", pattern);
        Some(Regex::new(&pattern).context("Invalid regex provided")?)
    } else {
        println!("Filter Pattern:   None (All files)");
        None
    };

    println!("Target Directory: {:?}", target_dir);

    // Config
    let config = SplicerConfig {
        chunk_size: 256 * 1024,
        max_buffer_size: 1024 * 1024,
        path_filter,
        recursive: true,
    };

    let word_counts = Arc::new(DashMap::new());
    let stats = Arc::new(SplicerStats::default());
    
    // Channel size is critical. Too small = Splicers block. Too big = High RAM usage.
    let (tx, rx) = bounded::<String>(256);

    // --- Monitor Thread ---
    let stats_monitor = stats.clone();
    thread::spawn(move || {
        let start = std::time::Instant::now();
        let mut last_bytes = 0;
        loop {
            thread::sleep(Duration::from_secs(1));
            let elapsed = start.elapsed().as_secs_f64();
            let files = stats_monitor.file_count.load(Ordering::Relaxed);
            let chunks = stats_monitor.chunk_count.load(Ordering::Relaxed);
            let bytes = stats_monitor.byte_count.load(Ordering::Relaxed);
            
            let mb_total = bytes as f64 / 1024.0 / 1024.0;
            let mb_diff = (bytes - last_bytes) as f64 / 1024.0 / 1024.0;
            last_bytes = bytes;

            println!(
                "Stats: [Files: {}] [Chunks: {}] [Total: {:.2} MB] [Rate: {:.2} MB/s]", 
                files, chunks, mb_total, mb_diff // Instantaneous rate is often more useful
            );
        }
    });

    // --- Worker Threads (Consumers) ---
    // These parse the strings and update the map
    let num_workers = std::thread::available_parallelism().unwrap().get();
    println!("Starting {} consumer threads...", num_workers);
    
    let mut handles = vec![];
    
    for _ in 0..num_workers {
        let rx_worker = rx.clone();
        let map_worker = word_counts.clone();
        
        handles.push(thread::spawn(move || {
            while let Ok(chunk) = rx_worker.recv() {
                for word in chunk.split_whitespace() {
                    let w = word
                        .trim_matches(|c: char| !c.is_alphanumeric())
                        .to_lowercase();
                    if !w.is_empty() {
                        *map_worker.entry(w).or_insert(0) += 1;
                    }
                }
            }
        }));
    }

    // --- Splicer (Producers) ---
    // The splicer now internally manages a thread pool to read files in parallel
    let splicer = IoSplicer::new(config, stats.clone(), tx);
    
    println!("Starting splicer...");
    // This blocks until all files are processed
    if let Err(e) = splicer.run(InputSource::Directory(target_dir)) {
        eprintln!("Splicer error: {:?}", e);
    }
    
    println!("Splicing complete. Waiting for consumers...");
    drop(splicer); // Close channel

    for h in handles {
        h.join().unwrap();
    }

    // --- Results ---
    println!("\n--- Processing Complete ---");
    println!("Total Unique Words: {}", word_counts.len());
    
    let mut count_vec: Vec<_> = word_counts.iter()
        .map(|e| (e.key().clone(), *e.value()))
        .collect();
    
    // Sort descending
    count_vec.sort_by(|a, b| b.1.cmp(&a.1));

    println!("Top 20 Words:");
    for (i, (word, count)) in count_vec.iter().take(20).enumerate() {
        println!("{}. {}: {}", i + 1, word, count);
    }

    Ok(())
}