use ozone_rust::{ClientConfig, OzoneClient};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::time::{Duration, Instant};
use uuid::Uuid;

fn om_endpoint() -> String {
    std::env::var("OZONE_OM_ENDPOINT").unwrap()
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

fn test_payload() -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(0x0A0B_0C0D);
    let mut bytes = vec![0; 2 * 1024 * 1024 + 137];
    rng.fill_bytes(&mut bytes);
    bytes
}

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
#[ignore = "requires a local docker-compose Ozone cluster"]
fn test_volume_bucket_key_metadata_and_roundtrip() -> TestResult {
    std::thread::Builder::new()
        .name("ozone-cluster-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async { test_volume_bucket_key_metadata_and_roundtrip_impl().await })
        })?
        .join()
        .expect("integration test thread")
}

#[test]
#[ignore = "requires a local docker-compose Ozone cluster"]
fn test_large_key_write_uses_pipeline_config_and_roundtrips() -> TestResult {
    std::thread::Builder::new()
        .name("ozone-pipeline-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async {
                let client = OzoneClient::connect_with_config(
                    &om_endpoint(),
                    ClientConfig {
                        host_override: Some("127.0.0.1".to_string()),
                        chunk_size: 64 * 1024,
                        stream_flush_size: 256 * 1024,
                        stream_window_size: 512 * 1024,
                        ..ClientConfig::default()
                    },
                )
                .await?;
                let volume = unique_name("vol");
                let bucket = unique_name("bucket");
                let key = unique_name("key");
                let data = vec![0x5a; 2 * 1024 * 1024 + 137];

                client.create_volume(&volume, "ozone", "ozone").await?;
                client.create_bucket(&volume, &bucket).await?;

                let written = client.put_key_bytes(&volume, &bucket, &key, &data).await?;
                let roundtrip = client.get_key_bytes(&volume, &bucket, &key).await?;

                assert_eq!(written.data_size, data.len() as u64);
                assert_eq!(roundtrip, data);

                client.delete_key(&volume, &bucket, &key).await?;
                client.delete_bucket(&volume, &bucket).await?;
                client.delete_volume(&volume).await?;
                Ok(())
            })
        })?
        .join()
        .expect("integration test thread")
}

async fn test_volume_bucket_key_metadata_and_roundtrip_impl() -> TestResult {
    let client = OzoneClient::connect_with_config(
        &om_endpoint(),
        ClientConfig {
            host_override: Some("127.0.0.1".to_string()),
            ..ClientConfig::default()
        },
    )
    .await?;
    let volume = unique_name("vol");
    let bucket = unique_name("bucket");
    let key = unique_name("key");
    let data = test_payload();

    client.create_volume(&volume, "ozone", "ozone").await?;
    let volume_info = client.info_volume(&volume).await?;
    assert_eq!(volume_info.volume, volume);
    assert!(client
        .list_volumes(Some(&volume))
        .await?
        .iter()
        .any(|candidate| candidate.volume == volume));

    client.create_bucket(&volume, &bucket).await?;
    let bucket_info = client.info_bucket(&volume, &bucket).await?;
    assert_eq!(bucket_info.bucket_name, bucket);
    assert!(client
        .list_buckets(&volume, Some(&bucket))
        .await?
        .iter()
        .any(|candidate| candidate.bucket_name == bucket));

    let written = client.put_key_bytes(&volume, &bucket, &key, &data).await?;
    assert_eq!(written.key_name, key);
    assert_eq!(written.data_size, data.len() as u64);

    let looked_up = client.get_key_info(&volume, &bucket, &key).await?;
    assert_eq!(looked_up.key_name, key);
    assert_eq!(looked_up.data_size, data.len() as u64);
    assert!(wait_for_key_listing(&client, &volume, &bucket, &key, Duration::from_secs(5)).await?);
    let roundtrip = client.get_key_bytes(&volume, &bucket, &key).await?;
    assert_eq!(roundtrip, data);

    client.delete_key(&volume, &bucket, &key).await?;
    client.delete_bucket(&volume, &bucket).await?;
    client.delete_volume(&volume).await?;
    Ok(())
}

async fn wait_for_key_listing(
    client: &OzoneClient,
    volume: &str,
    bucket: &str,
    key: &str,
    timeout: Duration,
) -> Result<bool, ozone_rust::Error> {
    let deadline = Instant::now() + timeout;
    loop {
        let listed = client
            .list_keys(volume, bucket, Some(key))
            .await?
            .iter()
            .any(|candidate| candidate.key_name == key);
        if listed {
            return Ok(true);
        }

        if Instant::now() >= deadline {
            return Ok(false);
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
