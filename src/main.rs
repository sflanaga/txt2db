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
    // 1. Setup
    let args: Vec<String> = env::args().collect();
    let target_dir = args.get(1).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    
    let filter_arg = args.get(2).cloned();
    let path_filter = if let Some(pattern) = filter_arg {
        println!("Filter: {}", pattern);
        Some(Regex::new(&pattern).context("Invalid regex")?)
    } else {
        None
    };

    // 2. Resource Allocation
    let total_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    // 50/50 split between Splicing (IO/Decompression) and Parsing (CPU/Hashing)
    let splicer_threads = std::cmp::max(1, total_cores / 2);
    let parser_threads = std::cmp::max(1, total_cores - splicer_threads);

    println!("Configuration: {} Total Cores", total_cores);
    println!("  - Splicer Pool: {} threads", splicer_threads);
    println!("  - Parser Pool:  {} threads", parser_threads);

    let config = SplicerConfig {
        chunk_size: 256 * 1024,
        max_buffer_size: 1024 * 1024,
        path_filter,
        recursive: true,
        thread_count: splicer_threads,
    };

    let word_counts = Arc::new(DashMap::new());
    let stats = Arc::new(SplicerStats::default());
    
    // Chunk Queue: Splicer -> Parser
    let (tx, rx) = bounded::<String>(256); 

    // 3. Stats Monitor
    let stats_monitor = stats.clone();
    let rx_monitor = rx.clone();
    let queue_capacity = rx.capacity().unwrap_or(0);

    thread::spawn(move || {
        let start = std::time::Instant::now();
        let mut last_bytes = 0;
        loop {
            thread::sleep(Duration::from_secs(1));
            let elapsed = start.elapsed().as_secs_f64();
            let files = stats_monitor.file_count.load(Ordering::Relaxed);
            let bytes = stats_monitor.byte_count.load(Ordering::Relaxed);
            
            let q_len = rx_monitor.len();
            let q_pct = (q_len as f64 / queue_capacity as f64) * 100.0;
            let file_q = stats_monitor.paths_queued.load(Ordering::Relaxed);
            
            let mb_total = bytes as f64 / 1024.0 / 1024.0;
            let mb_diff = (bytes - last_bytes) as f64 / 1024.0 / 1024.0;
            last_bytes = bytes;

            println!(
                "Status: [Files: {:5}] [MB: {:6.1}] [Speed: {:5.1} MB/s] [File Q: {:3}] [Parse Q: {:3}/{:3} ({:3.0}%)]", 
                files, mb_total, mb_diff, file_q, q_len, queue_capacity, q_pct
            );
        }
    });

    // 4. Start Parser Workers
    let mut handles = vec![];
    for _ in 0..parser_threads {
        let rx_worker = rx.clone();
        let map_worker = word_counts.clone();
        handles.push(thread::spawn(move || {
            while let Ok(chunk) = rx_worker.recv() {
                for word in chunk.split_whitespace() {
                    let w = word.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase();
                    if !w.is_empty() {
                        *map_worker.entry(w).or_insert(0) += 1;
                    }
                }
            }
        }));
    }

    // 5. Run Splicer
    println!("Starting scan on {:?}...", target_dir);
    let start_time = std::time::Instant::now();
    let splicer = IoSplicer::new(config, stats.clone(), tx);
    
    if let Err(e) = splicer.run(InputSource::Directory(target_dir)) {
        eprintln!("Splicer Error: {:?}", e);
    }
    drop(splicer); // EOF signal

    for h in handles {
        h.join().unwrap();
    }
    
    let duration = start_time.elapsed();

    // 6. FINAL SUMMARY STATS
    let final_files = stats.file_count.load(Ordering::Relaxed);
    let final_bytes = stats.byte_count.load(Ordering::Relaxed);
    let final_chunks = stats.chunk_count.load(Ordering::Relaxed);
    let mb = final_bytes as f64 / 1024.0 / 1024.0;
    let seconds = duration.as_secs_f64();

    println!("\n================ FINAL STATS ================");
    println!("Duration:       {:.2} seconds", seconds);
    println!("Total Files:    {}", final_files);
    println!("Total Chunks:   {}", final_chunks);
    println!("Total Size:     {:.2} MB", mb);
    println!("Avg Throughput: {:.2} MB/s", mb / seconds);
    println!("Unique Words:   {}", word_counts.len());
    println!("=============================================");

    // Top 20 Words
    let mut count_vec: Vec<_> = word_counts.iter().map(|e| (e.key().clone(), *e.value())).collect();
    count_vec.sort_by(|a, b| b.1.cmp(&a.1));
    println!("\nTop 20 Words:");
    for (i, (w, c)) in count_vec.iter().take(20).enumerate() {
        println!("{}. {}: {}", i + 1, w, c);
    }

    Ok(())
}