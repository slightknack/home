//! ed25519 keys, blake3 hashes, XChaCha20-Poly1305

use ed25519_dalek::Signer;
use ed25519_dalek::SigningKey;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::Verifier;
use ed25519_dalek::Signature as Ed25519Signature;
use x25519_dalek::StaticSecret;
use x25519_dalek::PublicKey as X25519PublicKey;
use blake3::Hasher;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::XNonce;
use chacha20poly1305::aead::KeyInit;
use chacha20poly1305::XChaCha20Poly1305;
use argon2::Argon2;
use argon2::PasswordHasher;
use argon2::password_hash::SaltString;
use rand_core::OsRng;
use rand_core::RngCore;

#[derive(Clone, PartialEq, Eq)]
pub struct KeyPub(pub [u8; 32]);

#[derive(Clone, PartialEq, Eq)]
pub struct KeySec(pub [u8; 32]);

#[derive(Clone, Debug)]
pub struct KeyPair {
    pub key_pub: KeyPub,
    pub key_sec: KeySec,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Hash(pub [u8; 32]);

impl std::fmt::Debug for KeyPub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyPub({})", hex(&self.0))
    }
}

impl std::fmt::Debug for KeySec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeySec({})", hex(&self.0))
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", hex(&self.0))
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn hash(message: &[u8]) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(message);
    let result = hasher.finalize();
    Hash(*result.as_bytes())
}

pub fn generate_nonce() -> [u8; 24] {
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

impl KeyPair {
    /// Generate a random keypair
    pub fn ephemeral() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        KeyPair {
            key_sec: KeySec(signing_key.to_bytes()),
            key_pub: KeyPub(verifying_key.to_bytes()),
        }
    }

    /// Generate from a password, using Argon2
    pub fn from_password(salt: &[u8], password: &[u8]) -> Self {
        let argon2 = Argon2::default();

        // Create a fixed-length salt string
        let salt_string = SaltString::encode_b64(salt).unwrap();

        // Hash the password
        let password_hash = argon2
            .hash_password(password, &salt_string)
            .unwrap();

        // Extract the hash bytes
        let hash_bytes = password_hash.hash.unwrap();
        let hash_slice = hash_bytes.as_bytes();

        // Use first 32 bytes as secret key
        let mut sec_bytes = [0u8; 32];
        let len = hash_slice.len().min(32);
        sec_bytes[..len].copy_from_slice(&hash_slice[..len]);

        // Derive signing key from the hash
        let signing_key = SigningKey::from_bytes(&sec_bytes);
        let verifying_key = signing_key.verifying_key();

        KeyPair {
            key_sec: KeySec(sec_bytes),
            key_pub: KeyPub(verifying_key.to_bytes()),
        }
    }

    /// Derives a shared secret using X25519
    pub fn conspire(&self, other: &KeyPub) -> KeyShared {
        // Convert Ed25519 secret key to X25519 format
        let signing_key = SigningKey::from_bytes(&self.key_sec.0);
        let secret_scalar = signing_key.to_scalar_bytes();
        let x25519_secret = StaticSecret::from(secret_scalar);

        // Convert Ed25519 public key to X25519 (Montgomery) format
        let verifying_key = VerifyingKey::from_bytes(&other.0).unwrap();
        let montgomery_point = verifying_key.to_montgomery();
        let x25519_public = X25519PublicKey::from(*montgomery_point.as_bytes());

        // Perform X25519 Diffie-Hellman
        let shared = x25519_secret.diffie_hellman(&x25519_public);
        KeyShared(*shared.as_bytes())
    }

    /// Encrypt a message to another party using X25519 key exchange
    pub fn encrypt(&self, other: &KeyPub, message: &[u8]) -> Payload {
        self.conspire(other).encrypt(message)
    }

    /// Decrypt a message from another party using X25519 key exchange
    pub fn decrypt(&self, other: &KeyPub, payload: Payload) -> Result<Vec<u8>, DecryptError> {
        self.conspire(other).decrypt(payload)
    }

    /// For encryption at rest - derive a key from own keypair
    pub fn at_rest(&self) -> KeyShared {
        KeyShared(blake3::derive_key("home encryption-at-rest v1", &self.key_sec.0))
    }

    /// Encrypt a message for yourself (encryption at rest)
    pub fn encrypt_rest(&self, message: &[u8]) -> Payload {
        self.at_rest().encrypt(message)
    }

    /// Decrypt a message encrypted for yourself (encryption at rest)
    pub fn decrypt_rest(&self, payload: Payload) -> Result<Vec<u8>, DecryptError> {
        self.at_rest().decrypt(payload)
    }

    /// Sign a message using Ed25519
    pub fn sign(&self, message: &[u8]) -> Signature {
        let signing_key = SigningKey::from_bytes(&self.key_sec.0);
        Signature(signing_key.sign(message).to_bytes())
    }

    /// Verify a signature using Ed25519
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&self.key_pub.0);
        match verifying_key {
            Ok(vk) => {
                let sig = Ed25519Signature::from_bytes(&signature.0);
                vk.verify(message, &sig).is_ok()
            }
            Err(_) => false,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Signature(pub [u8; 64]);

impl std::fmt::Debug for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Signature({})", hex(&self.0))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct KeyShared(pub [u8; 32]);

impl std::fmt::Debug for KeyShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyShared({})", hex(&self.0))
    }
}

#[derive(Clone)]
pub struct Payload {
    pub nonce: [u8; 24],
    pub ciphertext: Vec<u8>,
}

#[derive(Debug)]
pub enum DecryptError {
    AuthenticationFailed,
    InvalidPayload,
}

impl KeyShared {
    /// Encrypts a message using XChaCha20-Poly1305 AEAD.
    pub fn encrypt(&self, message: &[u8]) -> Payload {
        let cipher = XChaCha20Poly1305::new_from_slice(&self.0).unwrap();
        let nonce = generate_nonce();
        let nonce_obj = XNonce::from_slice(&nonce);
        let ciphertext = cipher.encrypt(nonce_obj, message).unwrap();

        Payload {
            nonce,
            ciphertext,
        }
    }

    /// Decrypts a message. Will return error if message is corrupt or forged.
    pub fn decrypt(&self, payload: Payload) -> Result<Vec<u8>, DecryptError> {
        let cipher = XChaCha20Poly1305::new_from_slice(&self.0).unwrap();
        let nonce = XNonce::from_slice(&payload.nonce);

        cipher
            .decrypt(nonce, payload.ciphertext.as_ref())
            .map_err(|_| DecryptError::AuthenticationFailed)
    }
}
