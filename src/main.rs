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
    /// Target directory to scan
    #[arg(default_value = ".")]
    target_dir: PathBuf,

    /// Optional Regex filter for file names (e.g., ".*\.java")
    #[arg(short = 'f', long = "filter")]
    filter_pattern: Option<String>,

    /// Number of threads for the splicer (IO/Decompression) pool.
    #[arg(short = 's', long = "splicers")]
    splicer_threads: Option<usize>,

    /// Number of threads for the parser (Word Count) pool.
    #[arg(short = 'p', long = "parsers")]
    parser_threads: Option<usize>,

    /// Optional specific string to count occurrences of.
    /// If set, the tool counts this string instead of all words.
    #[arg(short = 'q', long = "search")]
    search_term: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // 1. Setup Filters
    let path_filter = if let Some(pattern) = &cli.filter_pattern {
        println!("Filter: {}", pattern);
        Some(Regex::new(pattern).context("Invalid regex provided")?)
    } else {
        None
    };

    // 2. Resource Allocation
    let total_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    
    let splicer_threads = cli.splicer_threads.unwrap_or_else(|| std::cmp::max(1, total_cores / 2));
    let parser_threads = cli.parser_threads.unwrap_or_else(|| std::cmp::max(1, total_cores.saturating_sub(splicer_threads)));

    println!("Configuration: {} Cores", total_cores);
    println!("  - Splicer Pool: {}", splicer_threads);
    println!("  - Parser Pool:  {}", parser_threads);

    if let Some(term) = &cli.search_term {
        println!("  - Mode: Search for '{}'", term);
    } else {
        println!("  - Mode: Word Frequency Count");
    }

    let config = SplicerConfig {
        chunk_size: 256 * 1024,
        max_buffer_size: 1024 * 1024,
        path_filter,
        recursive: true,
        thread_count: splicer_threads,
    };

    let word_counts = Arc::new(DashMap::new());
    let stats = Arc::new(SplicerStats::default());
    
    // Chunk Queue
    let (tx, rx) = bounded::<String>(256); 

    // 3. Stats Monitor
    let stats_monitor = stats.clone();
    let rx_monitor = rx.clone();
    let queue_capacity = rx.capacity().unwrap_or(0);

    thread::spawn(move || {
        let mut last_bytes = 0;
        loop {
            thread::sleep(Duration::from_secs(1));
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

    // 4. Start Workers
    let search_term = cli.search_term.clone(); // Clone for closure capture
    let mut handles = vec![];
    
    for _ in 0..parser_threads {
        let rx_worker = rx.clone();
        let map_worker = word_counts.clone();
        // Each worker gets a copy of the search term string
        let term_worker = search_term.clone(); 

        handles.push(thread::spawn(move || {
            while let Ok(chunk) = rx_worker.recv() {
                if let Some(term) = &term_worker {
                    // --- SEARCH MODE ---
                    // Simple substring count (non-overlapping)
                    let count = chunk.matches(term).count();
                    if count > 0 {
                        // We store the result under the search term itself
                        *map_worker.entry(term.clone()).or_insert(0) += count;
                    }
                } else {
                    // --- WORD COUNT MODE ---
                    for word in chunk.split_whitespace() {
                        let w = word.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase();
                        if !w.is_empty() {
                            *map_worker.entry(w).or_insert(0) += 1;
                        }
                    }
                }
            }
        }));
    }

    // 5. Run Splicer
    println!("Starting scan on {:?}...", cli.target_dir);
    let start_time = std::time::Instant::now();
    let splicer = IoSplicer::new(config, stats.clone(), tx);
    
    if let Err(e) = splicer.run(InputSource::Directory(cli.target_dir)) {
        eprintln!("Splicer Error: {:?}", e);
    }
    drop(splicer);

    for h in handles {
        h.join().unwrap();
    }
    
    let duration = start_time.elapsed();

    // 6. Final Summary
    let final_files = stats.file_count.load(Ordering::Relaxed);
    let final_bytes = stats.byte_count.load(Ordering::Relaxed);
    let mb = final_bytes as f64 / 1024.0 / 1024.0;
    let seconds = duration.as_secs_f64();

    println!("\n================ FINAL STATS ================");
    println!("Duration:       {:.2} seconds", seconds);
    println!("Total Files:    {}", final_files);
    println!("Total Size:     {:.2} MB", mb);
    println!("Avg Throughput: {:.2} MB/s", if seconds > 0.0 { mb / seconds } else { 0.0 });
    println!("=============================================");

    if let Some(term) = &cli.search_term {
        let count = word_counts.get(term).map(|v| *v).unwrap_or(0);
        println!("\nSearch Results:");
        println!("String: '{}'", term);
        println!("Found:  {} times", count);
    } else {
        println!("\nTop 20 Words:");
        let mut count_vec: Vec<_> = word_counts.iter().map(|e| (e.key().clone(), *e.value())).collect();
        count_vec.sort_by(|a, b| b.1.cmp(&a.1));
        for (i, (w, c)) in count_vec.iter().take(20).enumerate() {
            println!("{}. {}: {}", i + 1, w, c);
        }
    }

    Ok(())
}
