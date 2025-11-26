use home::isocore::IsoCore;
use home::key::KeyPair;
use std::path::PathBuf;

pub fn main() {
    let path = PathBuf::from("../cores/compact");
    let signer = KeyPair::ephemeral();
    
    println!("Creating new isocore at: {:?}", path);
    let mut isocore = IsoCore::create(path.clone(), &signer).unwrap();

    println!("Adding 64 messages...\n");
    for i in 0..64 {
        let message = format!("message {}", i);
        let hash = isocore.add_message(message.as_bytes(), &signer).unwrap();
        let hex_bytes = hash.to_hex();
        let hex = String::from_utf8_lossy(&hex_bytes);
        println!("Added message {}: {}", i, hex);
    }

    println!("\nFlushing cores to disk...");
    isocore.data_core.flush().unwrap();
    isocore.verkle_core.flush().unwrap();
    isocore.sig_core.flush().unwrap();

    println!("Total messages: {}", isocore.len().0);
    
    // Show file sizes
    println!("\n=== File Analysis ===");
    let data_path = path.join("data").join("core.nd");
    let verkle_path = path.join("verkle").join("core.nd");
    let sig_path = path.join("sig").join("core.nd");
    
    if let Ok(metadata) = std::fs::metadata(&data_path) {
        println!("data/core.nd: {} bytes", metadata.len());
    }
    if let Ok(metadata) = std::fs::metadata(&verkle_path) {
        println!("verkle/core.nd: {} bytes", metadata.len());
    }
    if let Ok(metadata) = std::fs::metadata(&sig_path) {
        println!("sig/core.nd: {} bytes", metadata.len());
    }
}
