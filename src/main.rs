use anyhow::{Context, Result};
use clap::Parser;
use crossbeam_channel::bounded;
use dashmap::DashMap;
use io_splicer_demo::{InputSource, IoSplicer, SplicerConfig, SplicerStats};
use regex::Regex;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(default_value = ".")]
    target_dir: PathBuf,

    #[arg(short = 'f', long = "filter")]
    filter_pattern: Option<String>,

    #[arg(short = 's', long = "splicers")]
    splicer_threads: Option<usize>,

    #[arg(short = 'p', long = "parsers")]
    parser_threads: Option<usize>,
    
    /// Optional search term to count instead of all words
    #[arg(short = 'q', long = "search")]
    search_term: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // 1. Setup Filters & Config
    let path_filter = if let Some(pattern) = cli.filter_pattern {
        println!("Filter: {}", pattern);
        Some(Regex::new(&pattern).context("Invalid regex provided")?)
    } else {
        None
    };

    let total_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let splicer_count = cli.splicer_threads.unwrap_or_else(|| std::cmp::max(1, total_cores / 2));
    let parser_count = cli.parser_threads.unwrap_or_else(|| std::cmp::max(1, total_cores.saturating_sub(splicer_count)));

    println!("Configuration:");
    println!("  - Target:       {:?}", cli.target_dir);
    println!("  - Splicer Pool: {} threads", splicer_count);
    println!("  - Parser Pool:  {} threads", parser_count);

    let config = SplicerConfig {
        chunk_size: 256 * 1024,
        max_buffer_size: 1024 * 1024,
        path_filter,
        recursive: true,
        thread_count: splicer_count,
    };

    let word_counts = Arc::new(DashMap::new());
    let search_counts = Arc::new(DashMap::new());
    let stats = Arc::new(SplicerStats::default());
    
    // 2. Setup CHANNELS
    // The main channel carries data (Vec<u8>)
    let (tx, rx) = bounded::<Vec<u8>>(256); 
    
    // The recycle channel carries empty buffers back to the Splicer
    let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(512);

    // 3. Stats Monitor
    let stats_monitor = stats.clone();
    let rx_monitor = rx.clone();
    let queue_capacity = rx.capacity().unwrap_or(0);

    thread::spawn(move || {
        let mut last_bytes = 0;
        let mut last_files = 0; // Track previous file count

        loop {
            thread::sleep(Duration::from_secs(1));
            
            let files = stats_monitor.file_count.load(Ordering::Relaxed);
            let bytes = stats_monitor.byte_count.load(Ordering::Relaxed);
            
            let q_len = rx_monitor.len();
            let q_pct = (q_len as f64 / queue_capacity as f64) * 100.0;
            let file_q = stats_monitor.paths_queued.load(Ordering::Relaxed);
            
            let mb_total = bytes as f64 / 1024.0 / 1024.0;
            let mb_diff = (bytes - last_bytes) as f64 / 1024.0 / 1024.0;
            let files_diff = files - last_files; // Files per second
            
            last_bytes = bytes;
            last_files = files;

            println!(
                "Status: [Files: {:5} ({:4}/s)] [MB: {:6.1}] [Speed: {:5.1} MB/s] [File Q: {:3}] [Parse Q: {:3}/{:3} ({:3.0}%)]", 
                files, files_diff, mb_total, mb_diff, file_q, q_len, queue_capacity, q_pct
            );
        }
    });

    // 4. Start Parser Workers
    let mut handles = vec![];
    let search_term = cli.search_term.clone();
    
    for _ in 0..parser_count {
        let rx_worker = rx.clone();
        let recycle_worker = recycle_tx.clone();
        
        let map_worker = word_counts.clone();
        let search_map = search_counts.clone();
        let term = search_term.clone();

        handles.push(thread::spawn(move || {
            while let Ok(chunk_vec) = rx_worker.recv() {
                let chunk_str = String::from_utf8_lossy(&chunk_vec);
                
                if let Some(t) = &term {
                    // Search Mode
                    let count = chunk_str.matches(t).count();
                    if count > 0 {
                        *search_map.entry(t.clone()).or_insert(0) += count;
                    }
                } else {
                    // Word Count Mode
                    for word in chunk_str.split_whitespace() {
                        let w = word.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase();
                        if !w.is_empty() {
                            *map_worker.entry(w).or_insert(0) += 1;
                        }
                    }
                }
                
                // Recycle buffer
                drop(chunk_str); 
                let _ = recycle_worker.send(chunk_vec);
            }
        }));
    }

    // 5. Run Splicer
    println!("Starting scan on {:?}...", cli.target_dir);
    let start_time = std::time::Instant::now();
    
    let splicer = IoSplicer::new(config, stats.clone(), tx, recycle_rx);
    
    if let Err(e) = splicer.run(InputSource::Directory(cli.target_dir)) {
        eprintln!("Splicer Error: {:?}", e);
    }
    drop(splicer);

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
    println!("Avg Throughput: {:.2} MB/s", if seconds > 0.0 { mb / seconds } else { 0.0 });
    
    if let Some(t) = cli.search_term {
         let c = search_counts.get(&t).map(|v| *v).unwrap_or(0);
         println!("Term '{}':      {}", t, c);
    } else {
         println!("Unique Words:   {}", word_counts.len());
         let mut count_vec: Vec<_> = word_counts.iter().map(|e| (e.key().clone(), *e.value())).collect();
         count_vec.sort_by(|a, b| b.1.cmp(&a.1));
         println!("\nTop 20 Words:");
         for (i, (w, c)) in count_vec.iter().take(20).enumerate() {
             println!("{}. {}: {}", i + 1, w, c);
         }
    }
    println!("=============================================");

    Ok(())
}
