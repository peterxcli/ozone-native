use bytes::Bytes;
use ozone_rust::{
    AclEntry, AclEntryScope, AclEntryType, Client, ClientBuilder, FsAction, Result, WriteOptions,
};

#[test]
fn write_options_match_hdfs_native_builder_shape() {
    let options = WriteOptions::default()
        .block_size(128)
        .replication(3)
        .permission(0o755)
        .overwrite(true)
        .create_parent(false);

    assert_eq!(options.block_size, Some(128));
    assert_eq!(options.replication, Some(3));
    assert_eq!(options.permission, 0o755);
    assert!(options.overwrite);
    assert!(!options.create_parent);
}

#[test]
fn builder_rejects_unknown_config_keys() {
    let result = ClientBuilder::new()
        .with_url("http://127.0.0.1:9874")
        .with_config(vec![("ozone.unknown", "true")])
        .build_config_for_tests();

    assert!(result
        .expect_err("unknown config")
        .to_string()
        .contains("unknown client config key"));
}

#[allow(dead_code)]
async fn hdfs_like_methods_are_callable(client: Client) -> Result<()> {
    let options = WriteOptions::default().overwrite(true).create_parent(true);
    let mut writer = client.create("vol", "bucket", "key", options).await?;
    writer.write(Bytes::from_static(b"hello")).await?;
    writer.close().await?;

    let mut reader = client.read("vol", "bucket", "key").await?;
    let _ = reader.read(5).await?;
    let mut buf = [0; 5];
    let _ = reader.read_buf(&mut buf).await?;
    let _ = reader.read_range(0, 1).await?;
    reader.read_range_buf(&mut buf[..1], 0).await?;
    let _ = reader.read_range_stream(0, 1);

    let _ = client.get_file_info("vol", "bucket", "key").await?;
    let _ = client.list_status("vol", "bucket", "", false).await?;
    let _ = client
        .list_status_iter("vol", "bucket", "", true)
        .into_stream();
    client.mkdirs("vol", "bucket", "dir", 0o755, true).await?;
    let _ = client.delete("vol", "bucket", "key", false).await?;
    client.set_times("vol", "bucket", "key", 1, 2).await?;

    let acl = AclEntry::new(
        AclEntryType::User,
        AclEntryScope::Access,
        FsAction::ReadWrite,
        Some("alice".to_string()),
    );
    client
        .modify_acl_entries("vol", "bucket", "key", vec![acl.clone()])
        .await?;
    client
        .remove_acl_entries("vol", "bucket", "key", vec![acl.clone()])
        .await?;
    client.set_acl("vol", "bucket", "key", vec![acl]).await?;
    client.remove_default_acl("vol", "bucket", "key").await?;
    client.remove_acl("vol", "bucket", "key").await?;
    let _ = client.get_acl_status("vol", "bucket", "key").await?;

    let _ = client.append("vol", "bucket", "key").await;
    let _ = client
        .rename("vol", "bucket", "a", "vol", "bucket", "b", false)
        .await;
    let _ = client
        .set_owner("vol", "bucket", "key", Some("u"), Some("g"))
        .await;
    let _ = client.set_permission("vol", "bucket", "key", 0o644).await;
    let _ = client.set_replication("vol", "bucket", "key", 3).await;
    let _ = client.get_content_summary("vol", "bucket", "key").await;
    let _ = client.glob_status("vol", "bucket", "*.txt").await;

    Ok(())
}
