use anyhow::Result;
use crossbeam_channel::bounded;
use io_splicer_demo::{InputSource, IoSplicer, SplicerConfig, SplicerStats};
use regex::Regex;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    // 1. Setup Configuration
    // In a real app, these would come from CLI args (e.g. using clap)
    let config = SplicerConfig {
        chunk_size: 256 * 1024,      // 256KB
        max_buffer_size: 1024 * 1024, // 1MB
        path_filter: Some(Regex::new(r"\.rs$|\.md$|\.txt$").unwrap()), // Only process text-like files
        recursive: true,
    };

    // 2. Setup Stats and Communication
    let stats = Arc::new(SplicerStats::default());
    let (tx, rx) = bounded::<String>(100); // Backpressure channel

    // 3. Start Ticker Thread (Reporting)
    let stats_ticker = stats.clone();
    thread::spawn(move || {
        let start = std::time::Instant::now();
        loop {
            thread::sleep(Duration::from_secs(1));
            let elapsed = start.elapsed().as_secs_f64();
            let files = stats_ticker.file_count.load(Ordering::Relaxed);
            let bytes = stats_ticker.byte_count.load(Ordering::Relaxed);
            let chunks = stats_ticker.chunk_count.load(Ordering::Relaxed);
            
            let mb = bytes as f64 / 1024.0 / 1024.0;
            println!(
                "Stats: [Files: {}] [Chunks: {}] [Bytes: {:.2} MB] [Rate: {:.2} MB/s]", 
                files, chunks, mb, mb / elapsed
            );
        }
    });

    // 4. Start Consumer Threads (The "Thread Pool")
    // These simulate the "other things to process" mentioned in the prompt.
    let mut handles = vec![];
    for id in 0..4 {
        let rx_worker = rx.clone();
        handles.push(thread::spawn(move || {
            while let Ok(chunk) = rx_worker.recv() {
                // Simulate work: e.g., counting lines or words in the chunk
                let line_count = chunk.lines().count();
                // To avoid spamming stdout, we just do busy work
                let _ = line_count; 
                // println!("Worker {} processed chunk with {} lines", id, line_count);
            }
        }));
    }

    // 5. Determine Input and Run Splicer
    // For this demo, we'll scan the current directory if no args, or a specific dir if arg provided
    let input = if let Some(arg) = std::env::args().nth(1) {
        InputSource::Directory(PathBuf::from(arg))
    } else {
        println!("No directory argument provided. Scanning current directory recursively...");
        InputSource::Directory(PathBuf::from("."))
    };

    println!("Starting IO Splicer...");
    let splicer = IoSplicer::new(config, stats.clone(), tx);
    
    // Run the splicer (blocks until all inputs are read)
    if let Err(e) = splicer.run(input) {
        eprintln!("Splicer error: {:?}", e);
    }

    // Drop the sender so the workers know to exit
    // (IoSplicer owns the sender, so it drops here when splicer goes out of scope)
    drop(splicer);

    // Wait for workers
    for h in handles {
        h.join().unwrap();
    }

    println!("Done.");
    
    // Final Stats
    let files = stats.file_count.load(Ordering::Relaxed);
    let bytes = stats.byte_count.load(Ordering::Relaxed);
    let chunks = stats.chunk_count.load(Ordering::Relaxed);
    println!("Final: {} files, {} bytes, {} chunks processed.", files, bytes, chunks);

    Ok(())
}
