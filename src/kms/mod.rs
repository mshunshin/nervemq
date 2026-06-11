//! Key Management Service (KMS) module for handling encryption operations.
//!
//! This module provides traits and types for managing cryptographic keys and performing
//! encryption/decryption operations in a generic way.

use std::{future::Future, pin::Pin};

pub mod aws;
pub mod memory;
pub mod sqlite;

/// Represents an in-progress key rotation operation.
///
/// Useful for updating encryption of stored data without downtime or data loss.
pub struct Rotation {
    key_id: String,
    new_key_id: String,
}

impl Rotation {
    pub fn new(key_id: String, new_key_id: String) -> Self {
        Self { key_id, new_key_id }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn new_key_id(&self) -> &str {
        &self.new_key_id
    }
}

/// A trait for types that can be used as key identifiers.
///
/// This trait is automatically implemented for any type that implements
/// the required serialization traits, allowing for flexible key ID types
/// across different KMS implementations.
pub trait KeyId
where
    Self: Clone + Into<String> + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
}

impl<T> KeyId for T where
    T: Clone + Into<String> + serde::Serialize + for<'de> serde::Deserialize<'de>
{
}

/// Core trait for key management operations.
///
/// This trait defines the interface for a key management service, providing
/// methods for:
/// - Encrypting and decrypting data
/// - Creating and deleting encryption keys
/// - Rotating keys safely
///
/// Implementations of this trait should handle the underlying cryptographic
/// operations and key management details for specific KMS providers.
pub trait KeyManager: Send + Sync + 'static {
    /// Encrypts the provided data using a key managed by this service.
    ///
    /// # Arguments
    /// * `data` - The data to encrypt, provided as any type implementing `bytes::Buf`
    ///
    /// # Returns
    /// An [`Encrypted`] instance containing the encrypted data and the ID of the key used
    fn encrypt(
        &self,
        key_id: &String,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = eyre::Result<Vec<u8>>>>>;

    /// Decrypts the provided data using the specified key.
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to use for decryption
    /// * `data` - The encrypted data to decrypt
    ///
    /// # Returns
    /// The decrypted data as [`Bytes`]
    fn decrypt(
        &self,
        key_id: &String,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = eyre::Result<Vec<u8>>>>>;

    /// Creates a new encryption key.
    ///
    /// # Returns
    /// The ID of the newly created key
    fn create_key(&self) -> Pin<Box<dyn Future<Output = eyre::Result<String>>>>;

    /// Deletes an existing encryption key.
    ///
    /// # Warning
    /// Deleting a key will make it impossible to decrypt any data that was encrypted with it.
    fn delete_key(&self, key_id: &String) -> Pin<Box<dyn Future<Output = eyre::Result<()>>>>;

    /// Begin a key rotation operation.
    ///
    /// This will generate a new key and return a handle to the rotation operation. The handle
    /// should be stored securely and used to complete the rotation operation. The new key will
    /// not be used until the rotation is completed.
    ///
    /// During the rotation operation, you should decrypt data using the old key and re-encrypt it
    /// using the new key. Then, call [`KeyManager::complete_rotation`] with the handle to complete the
    /// rotation and activate the new key.
    ///
    /// # Important
    /// It is recommended to perform the rotation operations in a database transaction to avoid
    /// attempting to decrypt data requiring the new key before it is activated.
    fn begin_rotation<'a>(
        &'a self,
        key_id: &String,
    ) -> Pin<Box<dyn Future<Output = eyre::Result<Rotation>> + 'a>> {
        let key_id = key_id.clone();
        Box::pin(async move {
            let new_key = self.create_key().await?;

            Ok(Rotation {
                key_id: key_id.clone(),
                new_key_id: new_key,
            })
        })
    }

    /// Complete a key rotation operation.
    ///
    /// This method finalizes a key rotation operation that was started with [`KeyManager::begin_rotation`].
    /// It validates the rotation handle and secret, then activates the new key for use.
    ///
    /// # Important
    /// Before calling this method, ensure that:
    /// 1. All necessary data has been re-encrypted with the new key
    /// 2. The rotation handle and secret have been kept secure
    /// 3. You are ready to permanently switch to using the new key
    ///
    /// After successful completion:
    /// - The old key will be deactivated
    /// - All future encryption operations will use the new key
    /// - The rotation handle will no longer be valid
    fn complete_rotation<'a>(
        &'a self,
        handle: Rotation,
    ) -> Pin<Box<dyn Future<Output = eyre::Result<()>> + 'a>> {
        Box::pin(async move { self.delete_key(&handle.key_id).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kms::{memory::InMemoryKeyManager, sqlite::SqliteKeyManager};

    /// The default `begin_rotation`/`complete_rotation` flow, as documented
    /// on the trait: mint a new key, re-encrypt under it, complete to retire
    /// the old key. Generic so both shipped managers run the same script.
    async fn rotation_retires_the_old_key(kms: impl KeyManager) {
        let old_key = kms.create_key().await.unwrap();
        let ciphertext = kms.encrypt(&old_key, b"long-lived".to_vec()).await.unwrap();

        let rotation = kms.begin_rotation(&old_key).await.unwrap();
        assert_eq!(rotation.key_id(), old_key);
        assert_ne!(rotation.new_key_id(), old_key);

        // Re-encrypt while both keys are live, as the rotation contract
        // requires, then complete.
        let plaintext = kms.decrypt(&old_key, ciphertext).await.unwrap();
        let new_key = rotation.new_key_id().to_string();
        let reencrypted = kms.encrypt(&new_key, plaintext.clone()).await.unwrap();

        kms.complete_rotation(rotation).await.unwrap();

        // The old key is gone; the re-encrypted data survives.
        assert!(kms.encrypt(&old_key, b"x".to_vec()).await.is_err());
        assert_eq!(kms.decrypt(&new_key, reencrypted).await.unwrap(), plaintext);
    }

    #[tokio::test]
    async fn sqlite_manager_rotation_retires_the_old_key() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        rotation_retires_the_old_key(SqliteKeyManager::new(pool).await.unwrap()).await;
    }

    #[tokio::test]
    async fn memory_manager_rotation_retires_the_old_key() {
        rotation_retires_the_old_key(InMemoryKeyManager::new()).await;
    }

    #[test]
    fn rotation_accessors_expose_both_key_ids() {
        let rotation = Rotation::new("old".to_string(), "new".to_string());
        assert_eq!(rotation.key_id(), "old");
        assert_eq!(rotation.new_key_id(), "new");
    }
}
