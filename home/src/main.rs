use home::isocore::IsoCore;
use home::key::KeyPair;
use std::path::PathBuf;

pub fn main() {
    let path = PathBuf::from("../cores/test");
    let signer = KeyPair::ephemeral();
    let mut isocore = IsoCore::create(path, &signer).unwrap();

    for i in 0..64 {
        let message = format!("message {}", i);
        let hash = isocore.add_message(message.as_bytes(), &signer).unwrap();
        let hex_bytes = hash.to_hex();
        let hex = String::from_utf8_lossy(&hex_bytes);
        println!("Added message {}: {}", i, hex);
    }

    isocore.data_core.flush().unwrap();
    isocore.verkle_core.flush().unwrap();
    isocore.sig_core.flush().unwrap();

    println!("\nTotal messages: {}", isocore.len().0);
}
