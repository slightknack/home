//! Inspect neodisk file format

use std::fs::File;
use std::io::Read;
use home::jumpheader::FrameHeader;
use home::neopack::{Cursor, Decoder};

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        "../cores/compact/data.nd".to_string()
    });
    
    println!("Inspecting: {}\n", path);
    
    let mut f = File::open(&path).unwrap();
    let mut data = Vec::new();
    f.read_to_end(&mut data).unwrap();
    
    println!("Total file size: {} bytes\n", data.len());
    
    let mut pos = 0;
    let mut frame_num = 0;
    
    // Check for footer
    if data.len() < 16 {
        println!("ERROR: File too small for footer!");
        return;
    }
    
    let footer_start = data.len() - 16;
    let last_frame_offset = u64::from_le_bytes(data[footer_start..footer_start + 8].try_into().unwrap());
    let magic = &data[footer_start + 8..footer_start + 16];
    
    println!("=== Footer ===");
    println!("Last frame offset: 0x{:x} ({} bytes)", last_frame_offset, last_frame_offset);
    println!("Magic: {:?}", std::str::from_utf8(magic).unwrap_or("<invalid>"));
    println!();
    
    while pos < footer_start {
        println!("=== Frame {} ===", frame_num);
        println!("Header offset: 0x{:x} ({} bytes)", pos, pos);
        
        let cursor = Cursor::new(&data[pos..]);
        let mut decoder = Decoder::with_cursor(cursor);
        
        match decoder.raw_value() {
            Ok(header_bytes) => {
                match FrameHeader::decode(header_bytes) {
                    Ok(header) => {
                        println!("Frame number: {}", header.frame_number);
                        println!("Compressed size: {} bytes", header.compressed_size);
                        println!("Decompressed size: {} bytes", header.decompressed_size);
                        println!("Jump offsets: {:?}", header.jump_offsets);
                        
                        let header_size = decoder.pos();
                        println!("Header size: {} bytes", header_size);
                        pos += header_size;
                        
                        // Skip compressed data
                        if pos + header.compressed_size as usize > footer_start {
                            println!("ERROR: Compressed data extends beyond footer!");
                            break;
                        }
                        pos += header.compressed_size as usize;
                    }
                    Err(e) => {
                        println!("ERROR: Failed to decode header: {:?}", e);
                        break;
                    }
                }
            }
            Err(e) => {
                println!("ERROR: Failed to read header bytes: {:?}", e);
                break;
            }
        }
        
        println!();
        frame_num += 1;
    }
    
    println!("Total frames: {}", frame_num);
}
