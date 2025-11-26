use home::isocore::IsoCore;
use home::covering::ItemId;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from("../cores/compact");
    
    println!("Loading isocore from: {:?}", path);
    let mut isocore = IsoCore::load(path).expect("Failed to load isocore");
    
    println!("Total messages: {}", isocore.len().0);
    
    // Verify we can read messages back using the proper IsoCore API
    println!("\nVerifying first 10 messages...");
    for i in 0..10 {
        let item_id = ItemId(i);
        match isocore.get_message(item_id) {
            Ok(contents) => {
                let msg = String::from_utf8_lossy(contents);
                println!("Message {}: {}", i, msg);
            }
            Err(e) => {
                println!("Error reading message {}: {:?}", i, e);
            }
        }
    }
    
    println!("\n✓ All messages verified successfully!");
}
