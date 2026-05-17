use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const READY_PREFIX: &str = "Ready! ";
const MINIOZONE_MAIN_CLASS: &str = "dev.ozonerust.bench.MiniOzoneClusterLauncher";

pub struct BenchCluster {
    pub endpoint: String,
    pub om_rpc_endpoint: Option<String>,
    pub host_override: Option<String>,
    process: Option<Child>,
}

impl BenchCluster {
    pub fn start() -> Self {
        if let Ok(endpoint) = env::var("OZONE_OM_ENDPOINT") {
            return Self {
                endpoint,
                om_rpc_endpoint: env::var("OZONE_OM_RPC_ENDPOINT").ok(),
                host_override: env::var("OZONE_HOST_OVERRIDE").ok(),
                process: None,
            };
        }

        let grpc_port = free_local_port();
        build_miniozone_dependencies();
        let mut process = spawn_miniozone(grpc_port);
        let endpoints = read_ready_endpoint(&mut process);

        Self {
            endpoint: endpoints.grpc_endpoint,
            om_rpc_endpoint: endpoints.om_rpc_endpoint,
            host_override: Some(
                env::var("OZONE_HOST_OVERRIDE").unwrap_or_else(|_| "127.0.0.1".to_string()),
            ),
            process: Some(process),
        }
    }
}

struct ReadyEndpoints {
    grpc_endpoint: String,
    om_rpc_endpoint: Option<String>,
}

impl Drop for BenchCluster {
    fn drop(&mut self) {
        let Some(mut process) = self.process.take() else {
            return;
        };

        if let Some(stdin) = process.stdin.as_mut() {
            let _ = stdin.write_all(b"\n");
        }

        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            match process.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => thread::sleep(Duration::from_millis(200)),
                Err(_) => break,
            }
        }

        let _ = process.kill();
        let _ = process.wait();
    }
}

fn build_miniozone_dependencies() {
    if env::var("OZONE_BENCH_SKIP_MINIOZONE_BUILD").as_deref() == Ok("1") {
        return;
    }

    let status = Command::new("mvn")
        .current_dir(project_root())
        .args([
            "-f",
            "ozone/pom.xml",
            "-pl",
            "hadoop-ozone/mini-cluster",
            "-am",
            "-DskipTests",
            "-DskipRecon",
            "-Dskip.npm",
            "-Dskip.installnodenpm",
            "-Dskip.yarn",
            "-DskipShade",
            "-Drat.skip=true",
            "-Dcheckstyle.skip=true",
            "-Dspotbugs.skip=true",
            "-Dmaven.javadoc.skip=true",
            "install",
        ])
        .status()
        .expect("failed to start Maven while building Ozone mini-cluster dependencies");

    if !status.success() {
        panic!("failed to build Ozone mini-cluster dependencies with Maven");
    }
}

fn spawn_miniozone(grpc_port: u16) -> Child {
    let log_config = miniozone_log_config();
    eprintln!("MiniOzoneCluster log: {}", log_config.file.display());

    let exec_args = format!(
        "-Dorg.slf4j.simpleLogger.defaultLogLevel={} \
         -Dorg.slf4j.simpleLogger.showDateTime=true \
         -Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd_HH:mm:ss.SSS \
         -classpath %classpath {MINIOZONE_MAIN_CLASS}",
        log_config.level
    );
    let exec_property = format!("-Dexec.args={exec_args}");
    let mut process = Command::new("mvn")
        .current_dir(project_root())
        .args([
            "-f",
            "bench/miniozone/pom.xml",
            "--quiet",
            "compile",
            "exec:exec",
            "-Dexec.executable=java",
            exec_property.as_str(),
        ])
        .env("OZONE_BENCH_OM_GRPC_PORT", grpc_port.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start MiniOzoneCluster launcher with Maven");

    pipe_miniozone_stderr(&mut process, log_config.file);
    process
}

struct MiniOzoneLogConfig {
    file: PathBuf,
    level: String,
}

fn miniozone_log_config() -> MiniOzoneLogConfig {
    let dir = env::var("OZONE_BENCH_MINIOZONE_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| project_root().join("bench/miniozone/target/logs"));
    fs::create_dir_all(&dir).expect("create MiniOzoneCluster log directory");

    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis();
    let file = dir.join(format!("miniozone-{now_millis}-{}.log", process::id()));
    let level = env::var("OZONE_BENCH_MINIOZONE_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

    MiniOzoneLogConfig { file, level }
}

fn pipe_miniozone_stderr(process: &mut Child, log_file: PathBuf) {
    let mut stderr = process.stderr.take().expect("MiniOzoneCluster stderr");
    thread::spawn(move || {
        let mut file = match File::create(&log_file) {
            Ok(file) => file,
            Err(err) => {
                eprintln!(
                    "failed to create MiniOzoneCluster log {}: {err}",
                    log_file.display()
                );
                return;
            }
        };
        let _ = writeln!(
            file,
            "MiniOzoneCluster stderr log. Set OZONE_BENCH_MINIOZONE_LOG_LEVEL=debug for more detail."
        );
        let _ = io::copy(&mut stderr, &mut file);
    });
}

fn read_ready_endpoint(process: &mut Child) -> ReadyEndpoints {
    let stdout = process
        .stdout
        .take()
        .expect("MiniOzoneCluster launcher stdout");
    let mut lines = BufReader::new(stdout).lines();
    let mut seen = Vec::new();

    while let Some(line) = lines.next() {
        let line = line.expect("read MiniOzoneCluster launcher output");
        if let Some(endpoints) = line.strip_prefix(READY_PREFIX) {
            return parse_ready_endpoints(endpoints);
        }
        seen.push(line);
    }

    panic!(
        "MiniOzoneCluster launcher exited before reporting readiness. Output:\n{}",
        seen.join("\n")
    );
}

fn parse_ready_endpoints(endpoints: &str) -> ReadyEndpoints {
    if !endpoints.contains("grpc=") {
        return ReadyEndpoints {
            grpc_endpoint: endpoints.to_string(),
            om_rpc_endpoint: None,
        };
    }

    let mut grpc_endpoint = None;
    let mut om_rpc_endpoint = None;
    for part in endpoints.split_whitespace() {
        if let Some(endpoint) = part.strip_prefix("grpc=") {
            grpc_endpoint = Some(endpoint.to_string());
        } else if let Some(endpoint) = part.strip_prefix("rpc=") {
            om_rpc_endpoint = Some(endpoint.to_string());
        }
    }

    ReadyEndpoints {
        grpc_endpoint: grpc_endpoint.expect("MiniOzoneCluster did not report grpc endpoint"),
        om_rpc_endpoint,
    }
}

fn free_local_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind local port")
        .local_addr()
        .expect("read local port")
        .port()
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
