use anyhow::Result;
use crossbeam_channel::bounded;
use dashmap::DashMap;
use io_splicer_demo::{InputSource, IoSplicer, SplicerConfig, SplicerStats};
use regex::Regex;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    let config = SplicerConfig {
        chunk_size: 256 * 1024,
        max_buffer_size: 1024 * 1024,
        // Update regex to include compressed extensions
        path_filter: Some(Regex::new(r"\.(rs|md|txt|log|gz|bz2|zst)$").unwrap()),
        recursive: true,
    };

    let stats = Arc::new(SplicerStats::default());
    // DashMap is the "Java-like" concurrent map. It is sharded for high concurrency.
    let word_counts = Arc::new(DashMap::new());
    
    // Backpressure channel: prevent reading files infinitely faster than we can parse
    let (tx, rx) = bounded::<String>(50); 

    // --- Ticker Thread ---
    let stats_ticker = stats.clone();
    thread::spawn(move || {
        let start = std::time::Instant::now();
        loop {
            thread::sleep(Duration::from_secs(1));
            let elapsed = start.elapsed().as_secs_f64();
            let files = stats_ticker.file_count.load(Ordering::Relaxed);
            let bytes = stats_ticker.byte_count.load(Ordering::Relaxed);
            let mb = bytes as f64 / 1024.0 / 1024.0;
            
            println!(
                "Processing... [Files: {}] [MB Processed: {:.2}] [Avg Rate: {:.2} MB/s]", 
                files, mb, mb / elapsed
            );
        }
    });

    // --- Worker Pool (Word Counter) ---
    // Optimization: While DashMap is fast, high contention on very common words ("the", "a") 
    // can still slow things down. A common pattern is to aggregate locally in a HashMap 
    // inside the thread and merge into DashMap periodically or at the end.
    // However, since you asked for the DashMap usage specifically, we use it directly here.
    
    let mut handles = vec![];
    let num_threads = std::thread::available_parallelism().unwrap().get();
    
    for _ in 0..num_threads {
        let rx_worker = rx.clone();
        let map_worker = word_counts.clone();
        
        handles.push(thread::spawn(move || {
            while let Ok(chunk) = rx_worker.recv() {
                // Simple tokenizer: split by whitespace and cleanup punctuation
                for word in chunk.split_whitespace() {
                    let clean_word = word
                        .trim_matches(|c: char| !c.is_alphanumeric())
                        .to_lowercase();
                    
                    if !clean_word.is_empty() {
                        // DashMap handles the locking/sharding internally
                        *map_worker.entry(clean_word).or_insert(0) += 1;
                    }
                }
            }
        }));
    }

    // --- IO Splicer (Producer) ---
    let input = if let Some(arg) = std::env::args().nth(1) {
        InputSource::Directory(PathBuf::from(arg))
    } else {
        println!("Scanning current directory...");
        InputSource::Directory(PathBuf::from("."))
    };

    let splicer = IoSplicer::new(config, stats.clone(), tx);
    
    // This blocks until all files are read and chunks sent
    if let Err(e) = splicer.run(input) {
        eprintln!("Splicer error: {:?}", e);
    }
    
    // Drop the splicer (and its sender) so workers can exit loop
    drop(splicer);

    // Wait for all counting to finish
    for h in handles {
        h.join().unwrap();
    }

    // --- Final Report ---
    println!("\n--- Final Stats ---");
    println!("Files read: {}", stats.file_count.load(Ordering::Relaxed));
    println!("Total unique words: {}", word_counts.len());
    
    println!("\n--- Top 25 Words ---");
    // Extract to Vec to sort
    let mut count_vec: Vec<_> = word_counts
        .iter()
        .map(|pair| (pair.key().clone(), *pair.value()))
        .collect();
    
    // Sort descending by count
    count_vec.sort_by(|a, b| b.1.cmp(&a.1));

    for (i, (word, count)) in count_vec.iter().take(25).enumerate() {
        println!("{:2}. {:<20} : {}", i + 1, word, count);
    }

    Ok(())
}