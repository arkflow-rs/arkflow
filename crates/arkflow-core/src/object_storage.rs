/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Distributed object storage abstraction
//!
//! This module provides a unified interface for different object storage backends
//! including S3, Azure Blob Storage, Google Cloud Storage, and local file storage.

use crate::Error;
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Object metadata information
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: SystemTime,
    pub etag: Option<String>,
    pub metadata: HashMap<String, String>,
}

/// Storage configuration types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StorageType {
    S3(S3Config),
    MinIO(MinIOConfig),
    AzureBlob(AzureConfig),
    GCS(GCSConfig),
    Local(LocalConfig),
}

/// S3 storage configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub use_path_style: Option<bool>,
}

/// MinIO storage configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MinIOConfig {
    pub bucket: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: Option<String>,
    pub use_ssl: Option<bool>,
}

/// Azure Blob Storage configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AzureConfig {
    pub container: String,
    pub connection_string: Option<String>,
    pub account: Option<String>,
    pub access_key: Option<String>,
    pub endpoint: Option<String>,
}

/// Google Cloud Storage configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GCSConfig {
    pub bucket: String,
    pub credentials_path: Option<String>,
    pub project_id: Option<String>,
    pub endpoint: Option<String>,
}

/// Local file storage configuration (for testing and development)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LocalConfig {
    pub base_path: String,
}

/// Object storage trait that all backends must implement
#[async_trait]
pub trait ObjectStorage: Send + Sync {
    /// Upload an object to storage
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), Error>;

    /// Download an object from storage
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// Check if an object exists
    async fn exists(&self, key: &str) -> Result<bool, Error>;

    /// Delete an object from storage
    async fn delete_object(&self, key: &str) -> Result<(), Error>;

    /// List objects with a given prefix
    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error>;

    /// Get object metadata
    async fn get_object_info(&self, key: &str) -> Result<ObjectInfo, Error>;

    /// Copy an object to a new location
    async fn copy_object(&self, source: &str, destination: &str) -> Result<(), Error>;

    /// Upload multiple objects in batch
    async fn batch_put_objects(&self, objects: HashMap<String, Vec<u8>>) -> Result<(), Error> {
        // Default implementation - sequential upload
        for (key, data) in objects {
            self.put_object(&key, data).await?;
        }
        Ok(())
    }

    /// Get storage backend name
    fn storage_name(&self) -> &'static str;
}

/// Factory function to create object storage instances
pub async fn create_object_storage(
    storage_type: &StorageType,
) -> Result<Arc<dyn ObjectStorage>, Error> {
    match storage_type {
        StorageType::S3(config) => {
            let storage = S3Storage::new(config.clone()).await?;
            Ok(Arc::new(storage))
        }
        StorageType::MinIO(config) => {
            let storage = S3Storage::from_minio_config(config.clone()).await?;
            Ok(Arc::new(storage))
        }
        StorageType::AzureBlob(config) => {
            let storage = AzureStorage::new(config.clone()).await?;
            Ok(Arc::new(storage))
        }
        StorageType::GCS(config) => {
            let storage = GCSStorage::new(config.clone()).await?;
            Ok(Arc::new(storage))
        }
        StorageType::Local(config) => {
            let storage = LocalStorage::new(config.clone()).await?;
            Ok(Arc::new(storage))
        }
    }
}

/// S3 Storage implementation
pub struct S3Storage {
    client: aws_sdk_s3::Client,
    bucket: String,
    _endpoint: Option<String>,
}

impl S3Storage {
    pub async fn new(config: S3Config) -> Result<Self, Error> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region));

        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            config_loader = config_loader.credentials_provider(
                aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "static"),
            );
        }

        let aws_config = config_loader.load().await;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);

        // Configure custom endpoint if provided
        if let Some(endpoint) = &config.endpoint {
            s3_config = s3_config.endpoint_url(endpoint);
        }

        if let Some(use_path_style) = config.use_path_style {
            s3_config = s3_config.force_path_style(use_path_style);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        Ok(Self {
            client,
            bucket: config.bucket,
            _endpoint: config.endpoint,
        })
    }

    pub async fn from_minio_config(config: MinIOConfig) -> Result<Self, Error> {
        let region = config.region.unwrap_or_else(|| "us-east-1".to_string());
        let use_ssl = config.use_ssl.unwrap_or(false);
        let endpoint_url = if use_ssl {
            format!("https://{}", config.endpoint)
        } else {
            format!("http://{}", config.endpoint)
        };

        let s3_config = S3Config {
            bucket: config.bucket,
            region,
            endpoint: Some(endpoint_url),
            access_key_id: Some(config.access_key_id),
            secret_access_key: Some(config.secret_access_key),
            use_path_style: Some(true),
        };

        Self::new(s3_config).await
    }
}

#[async_trait]
impl ObjectStorage for S3Storage {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), Error> {
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data));

        request
            .send()
            .await
            .map_err(|e| Error::Unknown(format!("Failed to upload object to S3: {}", e)))?;

        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, Error> {
        let request = self.client.get_object().bucket(&self.bucket).key(key);

        let response = request
            .send()
            .await
            .map_err(|e| Error::Unknown(format!("Failed to download object from S3: {}", e)))?;

        let data =
            response.body.collect().await.map_err(|e| {
                Error::Unknown(format!("Failed to read object data from S3: {}", e))
            })?;

        Ok(data.into_bytes().to_vec())
    }

    async fn exists(&self, key: &str) -> Result<bool, Error> {
        let request = self.client.head_object().bucket(&self.bucket).key(key);

        match request.send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error()
                    .and_then(|se| se.code())
                    .map(|code| code == "NoSuchKey" || code == "NotFound")
                    .unwrap_or(false)
                {
                    Ok(false)
                } else {
                    Err(Error::Unknown(format!(
                        "Failed to check object existence in S3: {}",
                        e
                    )))
                }
            }
        }
    }

    async fn delete_object(&self, key: &str) -> Result<(), Error> {
        let request = self.client.delete_object().bucket(&self.bucket).key(key);

        request
            .send()
            .await
            .map_err(|e| Error::Unknown(format!("Failed to delete object from S3: {}", e)))?;

        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| Error::Unknown(format!("Failed to list objects in S3: {}", e)))?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    let object_info = ObjectInfo {
                        key: obj.key.unwrap_or_default(),
                        size: obj.size.unwrap_or(0) as u64,
                        last_modified: obj
                            .last_modified
                            .map(|dt| dt.try_into().unwrap_or(SystemTime::UNIX_EPOCH))
                            .unwrap_or(SystemTime::UNIX_EPOCH),
                        etag: obj.e_tag,
                        metadata: HashMap::new(),
                    };
                    objects.push(object_info);
                }
            }

            if response.next_continuation_token.is_none() {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        Ok(objects)
    }

    async fn get_object_info(&self, key: &str) -> Result<ObjectInfo, Error> {
        let request = self.client.head_object().bucket(&self.bucket).key(key);

        let response = request
            .send()
            .await
            .map_err(|e| Error::Unknown(format!("Failed to get object info from S3: {}", e)))?;

        Ok(ObjectInfo {
            key: key.to_string(),
            size: response.content_length.unwrap_or(0) as u64,
            last_modified: response
                .last_modified
                .map(|dt| dt.try_into().unwrap_or(SystemTime::UNIX_EPOCH))
                .unwrap_or(SystemTime::UNIX_EPOCH),
            etag: response.e_tag,
            metadata: response
                .metadata
                .unwrap_or_default()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }

    async fn copy_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        let copy_source = format!("{}/{}", self.bucket, source);
        let request = self
            .client
            .copy_object()
            .bucket(&self.bucket)
            .key(destination)
            .copy_source(&copy_source);

        request
            .send()
            .await
            .map_err(|e| Error::Unknown(format!("Failed to copy object in S3: {}", e)))?;

        Ok(())
    }

    fn storage_name(&self) -> &'static str {
        "S3"
    }
}

/// Azure Blob Storage implementation
pub struct AzureStorage {
    _connection_string: String,
    _container: String,
    _client: Arc<()>, // Placeholder for Azure storage client
}

impl AzureStorage {
    pub async fn new(config: AzureConfig) -> Result<Self, Error> {
        let connection_string = config.connection_string.clone().ok_or_else(|| {
            Error::Unknown("Azure storage configuration requires connection_string".to_string())
        })?;

        // Create a simple placeholder client for now
        // This can be replaced with actual Azure SDK implementation later
        Ok(Self {
            _connection_string: connection_string,
            _container: config.container,
            _client: Arc::new(()), // Placeholder
        })
    }
}

#[async_trait]
impl ObjectStorage for AzureStorage {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), Error> {
        Err(Error::Unknown(
            "Azure storage not implemented yet".to_string(),
        ))
    }

    async fn get_object(&self, _key: &str) -> Result<Vec<u8>, Error> {
        Err(Error::Unknown(
            "Azure storage not implemented yet".to_string(),
        ))
    }

    async fn exists(&self, _key: &str) -> Result<bool, Error> {
        Err(Error::Unknown(
            "Azure storage not implemented yet".to_string(),
        ))
    }

    async fn delete_object(&self, _key: &str) -> Result<(), Error> {
        Err(Error::Unknown(
            "Azure storage not implemented yet".to_string(),
        ))
    }

    async fn list_objects(&self, _prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        Err(Error::Unknown(
            "Azure storage not implemented yet".to_string(),
        ))
    }

    async fn get_object_info(&self, _key: &str) -> Result<ObjectInfo, Error> {
        Err(Error::Unknown(
            "Azure storage not implemented yet".to_string(),
        ))
    }

    async fn copy_object(&self, _source: &str, _destination: &str) -> Result<(), Error> {
        Err(Error::Unknown(
            "Azure storage not implemented yet".to_string(),
        ))
    }

    fn storage_name(&self) -> &'static str {
        "AzureBlob"
    }
}

/// Google Cloud Storage implementation
pub struct GCSStorage {
    client: google_cloud_storage::client::Client,
    bucket: String,
}

impl GCSStorage {
    pub async fn new(config: GCSConfig) -> Result<Self, Error> {
        let client = google_cloud_storage::client::Client::default();

        Ok(Self {
            client,
            bucket: config.bucket,
        })
    }
}

#[async_trait]
impl ObjectStorage for GCSStorage {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), Error> {
        Err(Error::Unknown("GCS upload not implemented".to_string()))
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, Error> {
        use google_cloud_storage::http::objects::get::GetObjectRequest;

        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: key.to_string(),
            ..Default::default()
        };

        let result = self
            .client
            .download_object(
                &request,
                &google_cloud_storage::http::objects::download::Range::default(),
            )
            .await
            .map_err(|e| Error::Unknown(format!("Failed to download object from GCS: {}", e)))?;

        Ok(result)
    }

    async fn exists(&self, key: &str) -> Result<bool, Error> {
        use google_cloud_storage::http::objects::get::GetObjectRequest;

        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: key.to_string(),
            ..Default::default()
        };

        match self.client.get_object(&request).await {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("NotFound") => Ok(false),
            Err(e) => Err(Error::Unknown(format!(
                "Failed to check object existence in GCS: {}",
                e
            ))),
        }
    }

    async fn delete_object(&self, key: &str) -> Result<(), Error> {
        use google_cloud_storage::http::objects::delete::DeleteObjectRequest;

        let request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            object: key.to_string(),
            ..Default::default()
        };

        self.client
            .delete_object(&request)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to delete object from GCS: {}", e)))?;

        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        use google_cloud_storage::http::objects::list::ListObjectsRequest;

        let request = ListObjectsRequest {
            bucket: self.bucket.clone(),
            prefix: Some(prefix.to_string()),
            ..Default::default()
        };

        let response = self
            .client
            .list_objects(&request)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to list objects in GCS: {}", e)))?;

        let mut objects = Vec::new();
        if let Some(items) = response.items {
            for item in items {
                let object_info = ObjectInfo {
                    key: item.name,
                    size: item.size as u64,
                    last_modified: item.updated.map_or(SystemTime::UNIX_EPOCH, |t| t.into()),
                    etag: Some(item.etag),
                    metadata: item.metadata.unwrap_or_default(),
                };
                objects.push(object_info);
            }
        }

        Ok(objects)
    }

    async fn get_object_info(&self, key: &str) -> Result<ObjectInfo, Error> {
        use google_cloud_storage::http::objects::get::GetObjectRequest;

        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: key.to_string(),
            ..Default::default()
        };

        let response =
            self.client.get_object(&request).await.map_err(|e| {
                Error::Unknown(format!("Failed to get object info from GCS: {}", e))
            })?;

        Ok(ObjectInfo {
            key: key.to_string(),
            size: response.size as u64,
            last_modified: response
                .updated
                .map_or(SystemTime::UNIX_EPOCH, |t| t.into()),
            etag: Some(response.etag),
            metadata: response.metadata.unwrap_or_default(),
        })
    }

    async fn copy_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        use google_cloud_storage::http::objects::copy::CopyObjectRequest;

        let request = CopyObjectRequest {
            destination_bucket: self.bucket.clone(),
            destination_object: destination.to_string(),
            source_bucket: self.bucket.clone(),
            source_object: source.to_string(),
            ..Default::default()
        };

        self.client
            .copy_object(&request)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to copy object in GCS: {}", e)))?;

        Ok(())
    }

    fn storage_name(&self) -> &'static str {
        "GCS"
    }
}

/// Local file storage implementation (for testing and development)
pub struct LocalStorage {
    base_path: String,
}

impl LocalStorage {
    pub async fn new(config: LocalConfig) -> Result<Self, Error> {
        tokio::fs::create_dir_all(&config.base_path)
            .await
            .map_err(|e| {
                Error::Unknown(format!("Failed to create local storage directory: {}", e))
            })?;

        Ok(Self {
            base_path: config.base_path,
        })
    }

    fn get_full_path(&self, key: &str) -> String {
        format!("{}/{}", self.base_path, key)
    }
}

#[async_trait]
impl ObjectStorage for LocalStorage {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), Error> {
        let full_path = self.get_full_path(key);
        let dir = std::path::Path::new(&full_path).parent().unwrap();

        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to create directory: {}", e)))?;

        tokio::fs::write(&full_path, data)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to write local file: {}", e)))?;

        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, Error> {
        let full_path = self.get_full_path(key);
        tokio::fs::read(&full_path)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to read local file: {}", e)))
    }

    async fn exists(&self, key: &str) -> Result<bool, Error> {
        let full_path = self.get_full_path(key);
        Ok(tokio::fs::metadata(&full_path).await.is_ok())
    }

    async fn delete_object(&self, key: &str) -> Result<(), Error> {
        let full_path = self.get_full_path(key);
        tokio::fs::remove_file(&full_path)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to delete local file: {}", e)))
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        let full_prefix = self.get_full_path(prefix);
        let prefix_path = std::path::Path::new(&full_prefix);

        let mut objects = Vec::new();

        if prefix_path.exists() && prefix_path.is_dir() {
            let mut entries = tokio::fs::read_dir(prefix_path)
                .await
                .map_err(|e| Error::Unknown(format!("Failed to read directory: {}", e)))?;

            while let Ok(Some(entry)) = entries.next_entry().await {
                let metadata = entry
                    .metadata()
                    .await
                    .map_err(|e| Error::Unknown(format!("Failed to get file metadata: {}", e)))?;

                let key = entry
                    .path()
                    .strip_prefix(&self.base_path)
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
                    .replace(std::path::MAIN_SEPARATOR, "/");

                let object_info = ObjectInfo {
                    key,
                    size: metadata.len(),
                    last_modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    etag: None,
                    metadata: HashMap::new(),
                };
                objects.push(object_info);
            }
        } else if prefix_path.exists() && prefix_path.is_file() {
            let metadata = tokio::fs::metadata(prefix_path)
                .await
                .map_err(|e| Error::Unknown(format!("Failed to get file metadata: {}", e)))?;

            let key = prefix_path
                .strip_prefix(&self.base_path)
                .unwrap()
                .to_string_lossy()
                .to_string()
                .replace(std::path::MAIN_SEPARATOR, "/");

            let object_info = ObjectInfo {
                key,
                size: metadata.len(),
                last_modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                etag: None,
                metadata: HashMap::new(),
            };
            objects.push(object_info);
        }

        Ok(objects)
    }

    async fn get_object_info(&self, key: &str) -> Result<ObjectInfo, Error> {
        let full_path = self.get_full_path(key);
        let metadata = tokio::fs::metadata(&full_path)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to get file metadata: {}", e)))?;

        Ok(ObjectInfo {
            key: key.to_string(),
            size: metadata.len(),
            last_modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            etag: None,
            metadata: HashMap::new(),
        })
    }

    async fn copy_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        let source_path = self.get_full_path(source);
        let dest_path = self.get_full_path(destination);

        tokio::fs::copy(&source_path, &dest_path)
            .await
            .map_err(|e| Error::Unknown(format!("Failed to copy local file: {}", e)))?;

        Ok(())
    }

    fn storage_name(&self) -> &'static str {
        "Local"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = LocalConfig {
            base_path: temp_dir.path().to_string_lossy().to_string(),
        };

        let storage = LocalStorage::new(config).await.unwrap();

        // Test put and get
        let test_data = b"test data".to_vec();
        storage
            .put_object("test/key", test_data.clone())
            .await
            .unwrap();

        let retrieved = storage.get_object("test/key").await.unwrap();
        assert_eq!(retrieved, test_data);

        // Test exists
        assert!(storage.exists("test/key").await.unwrap());
        assert!(!storage.exists("nonexistent").await.unwrap());

        // Test list objects
        let objects = storage.list_objects("test/").await.unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].key, "test/key");

        // Test delete
        storage.delete_object("test/key").await.unwrap();
        assert!(!storage.exists("test/key").await.unwrap());
    }
}
