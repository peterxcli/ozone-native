use std::env;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use super::bench_cluster::BenchCluster;

const JAVA_BENCH_MAIN_CLASS: &str = "dev.ozonerust.bench.JavaOzoneClientBenchmark";
const READY_LINE: &str = "Ready!";
const MEASURED_PREFIX: &str = "Measured! ";

pub struct JavaBenchClient {
    process: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl JavaBenchClient {
    pub fn start(cluster: &BenchCluster, file_mib: usize, parallelism: usize) -> Self {
        let endpoint = cluster.om_rpc_endpoint.as_ref().unwrap_or_else(|| {
            panic!(
                "Java Ozone client benchmarks require OZONE_OM_RPC_ENDPOINT when \
                 OZONE_OM_ENDPOINT points at an external cluster"
            )
        });
        let replication = env::var("OZONE_BENCH_REPLICATION").unwrap_or_else(|_| "1".to_string());
        let exec_args = format!(
            "-classpath %classpath {} {} {} {} {}",
            JAVA_BENCH_MAIN_CLASS, endpoint, file_mib, parallelism, replication
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to start Java Ozone client benchmark with Maven");

        let stdout = read_ready(&mut process);
        let stdin = process.stdin.take().expect("Java benchmark stdin");
        Self {
            process,
            stdin,
            stdout,
        }
    }

    pub fn measure(&mut self, operation: &str, iterations: u64) -> Duration {
        writeln!(self.stdin, "measure {operation} {iterations}")
            .expect("write Java benchmark command");
        self.stdin.flush().expect("flush Java benchmark command");

        let mut seen = Vec::new();
        loop {
            let mut line = String::new();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("read Java benchmark result");
            if read == 0 {
                panic!(
                    "Java Ozone client benchmark exited before reporting a result. Output:\n{}",
                    seen.join("\n")
                );
            }

            let line = line.trim_end();
            if let Some(nanos) = line.strip_prefix(MEASURED_PREFIX) {
                let nanos = nanos.parse().expect("parse Java benchmark duration");
                return Duration::from_nanos(nanos);
            }
            if let Some(error) = line.strip_prefix("Error! ") {
                panic!("Java Ozone client benchmark failed: {error}");
            }
            seen.push(line.to_string());
        }
    }
}

impl Drop for JavaBenchClient {
    fn drop(&mut self) {
        let _ = self.stdin.write_all(b"close\n");
        let _ = self.stdin.flush();

        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            match self.process.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => thread::sleep(Duration::from_millis(200)),
                Err(_) => break,
            }
        }

        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn read_ready(process: &mut Child) -> BufReader<ChildStdout> {
    let stdout = process.stdout.take().expect("Java benchmark stdout");
    let mut stdout = BufReader::new(stdout);
    let mut seen = Vec::new();

    loop {
        let mut line = String::new();
        let read = stdout
            .read_line(&mut line)
            .expect("read Java benchmark startup output");
        if read == 0 {
            panic!(
                "Java Ozone client benchmark exited before reporting readiness. Output:\n{}",
                seen.join("\n")
            );
        }

        let line = line.trim_end();
        if line == READY_LINE {
            return stdout;
        }
        seen.push(line.to_string());
    }
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
