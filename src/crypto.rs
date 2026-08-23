use std::sync::Arc;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use secrecy::ExposeSecret;

use crate::{config::Config, error::AppError};

#[derive(Clone, Default)]
pub struct CredentialCipher {
    cipher: Option<Arc<Aes256Gcm>>,
}

impl CredentialCipher {
    pub fn from_config(config: &Config) -> Result<Self, AppError> {
        let Some(secret) = &config.credential_key else {
            return Ok(Self::default());
        };
        let raw = secret.expose_secret();
        let key = if raw.len() == 64 && raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            hex::decode(raw).map_err(|_| AppError::bad_request("invalid credential master key"))?
        } else {
            STANDARD.decode(raw).map_err(|_| {
                AppError::bad_request("credential master key must be 32-byte base64 or 64-char hex")
            })?
        };
        if key.len() != 32 {
            return Err(AppError::bad_request(
                "credential master key must contain exactly 32 bytes",
            ));
        }
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(AppError::internal)?;
        Ok(Self {
            cipher: Some(Arc::new(cipher)),
        })
    }

    pub fn is_configured(&self) -> bool {
        self.cipher.is_some()
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String, AppError> {
        let cipher = self.cipher.as_ref().ok_or_else(|| {
            AppError::bad_request("DONKEY_CREDENTIAL_KEY is required for upstream credentials")
        })?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|_| AppError::internal(anyhow::anyhow!("credential encryption failed")))?;
        let mut sealed = Vec::with_capacity(nonce.len() + ciphertext.len());
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&ciphertext);
        Ok(STANDARD.encode(sealed))
    }

    pub fn decrypt(&self, encoded: &str) -> Result<String, AppError> {
        let cipher = self.cipher.as_ref().ok_or_else(|| {
            AppError::bad_request("DONKEY_CREDENTIAL_KEY is required to read upstream credentials")
        })?;
        let sealed = STANDARD
            .decode(encoded)
            .map_err(|_| AppError::internal(anyhow::anyhow!("credential ciphertext is invalid")))?;
        if sealed.len() <= 12 {
            return Err(AppError::internal(anyhow::anyhow!(
                "credential ciphertext is truncated"
            )));
        }
        let (nonce, ciphertext) = sealed.split_at(12);
        let nonce: [u8; 12] = nonce
            .try_into()
            .map_err(|_| AppError::internal(anyhow::anyhow!("credential nonce is invalid")))?;
        let plaintext = cipher
            .decrypt(&Nonce::from(nonce), ciphertext)
            .map_err(|_| AppError::internal(anyhow::anyhow!("credential decryption failed")))?;
        String::from_utf8(plaintext)
            .map_err(|_| AppError::internal(anyhow::anyhow!("credential plaintext is invalid")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypts_with_random_nonce_and_round_trips() {
        let cipher = CredentialCipher {
            cipher: Some(Arc::new(Aes256Gcm::new_from_slice(&[7_u8; 32]).unwrap())),
        };
        let first = cipher.encrypt("secret").unwrap();
        let second = cipher.encrypt("secret").unwrap();
        assert_ne!(first, second);
        assert_eq!(cipher.decrypt(&first).unwrap(), "secret");
    }
}
