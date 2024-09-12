use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest as GcsGetObjectRequest;
use google_cloud_storage::http::Error as GcsError;
use rusoto_credential::EnvironmentProvider;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectError as S3GetObjectError,
    GetObjectRequest as S3GetObjectRequest, S3Client, S3,
};
use tokio::io::AsyncReadExt;

// struct that defines the configuration of the RLDS dataset source on Google Cloud Storage

pub struct DatasetSource {
    bucket: String,
    object: String,
}

impl DatasetSource {
    pub fn new(bucket: String, object: String) -> Self {
        DatasetSource { bucket, object }
    }
    // read from yaml file
    pub fn from_yaml(yaml: &str) -> Self {
        let config: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        DatasetSource {
            bucket: config["bucket"].as_str().unwrap().to_string(),
            object: config["object"].as_str().unwrap().to_string(),
        }
    }
    // fetch data from Google Cloud Storage and return the data as bytes
    pub async fn fetch_gcloud_data(&self) -> Result<Vec<u8>, GcsError> {
        let config = ClientConfig::default().anonymous();
        let client = Client::new(config);
        let req = GcsGetObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.to_string(),
            ..Default::default()
        };
        let range = Range::default();
        client.download_object(&req, &range).await
    }
    pub async fn fetch_s3_data(&self) -> Result<Vec<u8>, RusotoError<S3GetObjectError>> {
        let region = Region::default();
        log::info!("Region: {:?}", region);
        let client = S3Client::new_with(
            rusoto_core::request::HttpClient::new().unwrap(),
            EnvironmentProvider::default(),
            region,
        );
        log::info!("Client created");
        let req = S3GetObjectRequest {
            bucket: self.bucket.clone(),
            key: self.object.clone(),
            ..Default::default()
        };
        log::info!("Fetching data from S3 bucket: {}", self.bucket);
        let response = client.get_object(req).await?;
        log::info!("Response: {:?}", response);
        let body = response.body.unwrap();
        let mut buffer = Vec::new();
        body.into_async_read().read_to_end(&mut buffer).await?;
        Ok(buffer)
    }
}

pub struct Dataset {
    pub sources: Vec<DatasetSource>,
    pub name: String,
    pub description: String,
}

impl Dataset {
    pub fn new(sources: Vec<DatasetSource>, name: String, description: String) -> Self {
        Dataset {
            sources,
            name,
            description,
        }
    }
}
