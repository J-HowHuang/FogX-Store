mod dataset;

#[tokio::main]
async fn main() {
    env_logger::init();
    log::info!("Creating dataset");
    let dataset = dataset::Dataset::new(
        vec![
            // dataset::DatasetSource::new(
            //     "gdm-robotics-open-x-embodiment".to_string(),
            //     "cmu_stretch".to_string(),
            // ),
            dataset::DatasetSource::new(
                "skulk-example".to_string(),
                "bottle.npy".to_string(),
            ),
        ],
        "cmu_stretch".to_string(),
        "description".to_string(),
    );
    log::info!("Fetching data from source");
    let data = dataset.sources[0].fetch_s3_data().await.unwrap();
    log::info!("Data: {:?}", data);
}
