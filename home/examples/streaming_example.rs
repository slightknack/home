//! Example: Streaming message parser with memory compaction
//! 
//! Demonstrates:
//! - Streaming bytes from a "network" (simulated)
//! - Parsing messages incrementally
//! - Processing and dropping messages
//! - Freeing old bytes via compaction
//! - Handling Pending errors when data incomplete

use home::neopack::{Encoder, Decoder, StreamBuffer, Error};

fn main() {
    println!("=== Streaming Parser Example ===\n");
    
    // Simulate a network stream that sends messages in chunks
    streaming_example();
}

fn streaming_example() {
    let mut stream = StreamBuffer::new();
    let mut processed_count = 0;
    
    println!("Phase 1: Receive first chunk of data");
    println!("─────────────────────────────────────");
    
    // Simulate receiving bytes from network (3 messages)
    let chunk1 = create_message_chunk(&[
        ("Alice", 100),
        ("Bob", 200),
        ("Charlie", 300),
    ]);
    
    println!("Received {} bytes", chunk1.len());
    stream.extend(&chunk1);
    println!("Stream buffer: {} bytes\n", stream.len());
    
    // Parse messages from buffer
    {
        let cursor = stream.cursor();
        let mut decoder = Decoder::with_cursor(cursor);
        let initial_remaining = decoder.remaining();
        
        loop {
            // Try to read a message
            match read_user_message(&mut decoder) {
                Ok((name, score)) => {
                    println!("✓ Parsed message: {} (score: {})", name, score);
                    
                    // DO SOMETHING WITH THE MESSAGE
                    process_message(&name, score);
                    
                    processed_count += 1;
                }
                Err(Error::Pending(need)) => {
                    println!("⏸  Need {} more bytes for next message", need);
                    break;
                }
                Err(e) => {
                    println!("✗ Error: {:?}", e);
                    break;
                }
            }
        }
        
        // Mark bytes as consumed based on how much the decoder advanced
        let bytes_consumed = initial_remaining - decoder.remaining();
        stream.mark_consumed(bytes_consumed);
    }
    
    println!("\nPhase 2: Free processed bytes (compaction)");
    println!("─────────────────────────────────────────");
    
    println!("Before compaction: {} bytes in buffer", stream.len());
    let freed = stream.compact();
    println!("After compaction:  {} bytes in buffer", stream.len());
    println!("Freed {} bytes ✓\n", freed);
    
    println!("Phase 3: Receive second chunk of data");
    println!("──────────────────────────────────────");
    
    // Receive more data (3 more messages)
    let chunk2 = create_message_chunk(&[
        ("David", 400),
        ("Eve", 500),
        ("Frank", 600),
    ]);
    
    println!("Received {} bytes", chunk2.len());
    stream.extend(&chunk2);
    println!("Stream buffer: {} bytes\n", stream.len());
    
    // Parse new messages
    {
        let cursor = stream.cursor();
        let mut decoder = Decoder::with_cursor(cursor);
        let initial_remaining = decoder.remaining();
        
        loop {
            match read_user_message(&mut decoder) {
                Ok((name, score)) => {
                    println!("✓ Parsed message: {} (score: {})", name, score);
                    process_message(&name, score);
                    
                    processed_count += 1;
                }
                Err(Error::Pending(need)) => {
                    println!("⏸  Need {} more bytes", need);
                    break;
                }
                Err(e) => {
                    println!("✗ Error: {:?}", e);
                    break;
                }
            }
        }
        
        let bytes_consumed = initial_remaining - decoder.remaining();
        stream.mark_consumed(bytes_consumed);
    }
    
    println!("\nPhase 4: Final compaction");
    println!("─────────────────────────");
    
    let freed = stream.compact();
    println!("Freed {} bytes", freed);
    println!("Final buffer size: {} bytes", stream.len());
    
    println!("\n=== Summary ===");
    println!("Total messages processed: {}", processed_count);
    println!("Peak memory usage: ~{} bytes", chunk1.len() + chunk2.len());
    println!("Final memory usage: {} bytes", stream.len());
}

/// Helper: Create a chunk of neopack-encoded messages
fn create_message_chunk(messages: &[(&str, u64)]) -> Vec<u8> {
    let mut chunk = Vec::new();
    
    for (name, score) in messages {
        let mut enc = Encoder::new();
        
        // Encode as a list with two elements [name, score]
        let mut list = enc.list().unwrap();
        list.str(name).unwrap();
        list.u64(*score).unwrap();
        list.finish().unwrap();
        
        chunk.extend_from_slice(enc.as_bytes());
    }
    
    chunk
}

/// Helper: Read a user message (name + score)
fn read_user_message(decoder: &mut Decoder) -> Result<(String, u64), Error> {
    let mut list = decoder.list()?;
    
    let name = list.next()?.ok_or(Error::Malformed)?.as_str()?.to_string();
    let score = list.next()?.ok_or(Error::Malformed)?.as_u64()?;
    
    Ok((name, score))
}

/// Simulate processing a message (just print it)
fn process_message(name: &str, score: u64) {
    println!("  → Processing: {} with score {}", name, score);
    // In real code: save to database, send response, etc.
}
