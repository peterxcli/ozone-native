use std::path::{Path, PathBuf};

const OZONE_PROTO_DIR: &str = "ozone/hadoop-ozone/interface-client/src/main/proto";
const HDDS_PROTO_DIR: &str = "ozone/hadoop-hdds/interface-client/src/main/proto";
const RATIS_PROTO_DIR: &str = "ratis/ratis-proto/src/main/proto";

const PROTO_DIRS: &[&str] = &[OZONE_PROTO_DIR, HDDS_PROTO_DIR, RATIS_PROTO_DIR];
const PROTO_FILES: &[&str] = &[
    "ozone/hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto",
    "ozone/hadoop-ozone/interface-client/src/main/proto/Security.proto",
    "ozone/hadoop-hdds/interface-client/src/main/proto/hdds.proto",
    "ozone/hadoop-hdds/interface-client/src/main/proto/DatanodeClientProtocol.proto",
    "ratis/ratis-proto/src/main/proto/Raft.proto",
    "ratis/ratis-proto/src/main/proto/Grpc.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    for proto in PROTO_FILES {
        println!("cargo:rerun-if-changed={proto}");
    }

    let includes: Vec<PathBuf> = PROTO_DIRS.iter().map(PathBuf::from).collect();
    let protos: Vec<PathBuf> = PROTO_FILES.iter().map(PathBuf::from).collect();

    for proto in &protos {
        if !Path::new(proto).exists() {
            return Err(format!(
                "missing proto file {}. initialize submodules with: git submodule update --init --depth 1 --recursive",
                proto.display()
            )
            .into());
        }
    }

    let mut config = prost_build::Config::new();
    config.compile_well_known_types();
    config.bytes([
        ".hadoop.common.TokenProto.identifier",
        ".hadoop.common.TokenProto.password",
        ".ratis.common.ClientMessageEntryProto.content",
        ".ratis.common.RaftPeerProto.id",
        ".ratis.common.RaftPeerIdProto.id",
        ".ratis.common.RaftGroupIdProto.id",
        ".ratis.common.StateMachineEntryProto.stateMachineData",
        ".ratis.common.StateMachineLogEntryProto.logData",
    ]);

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos_with_config(config, &protos, &includes)?;

    Ok(())
}
