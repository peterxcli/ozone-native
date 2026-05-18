package dev.ozonerust.bench;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OMConfigKeys;

public final class MiniOzoneClusterLauncher {
  private static final String READY_PREFIX = "Ready! ";
  private static final int DEFAULT_DATANODES = 1;
  private static final int DEFAULT_REPLICATION = 1;
  private static final int DEFAULT_READY_TIMEOUT_MS = 120_000;

  private MiniOzoneClusterLauncher() {
  }

  public static void main(String[] args) throws Exception {
    int datanodes = positiveIntEnv("OZONE_BENCH_DATANODES",
        DEFAULT_DATANODES);
    int replication = replicationEnv("OZONE_BENCH_REPLICATION",
        DEFAULT_REPLICATION);
    if (datanodes < replication) {
      datanodes = replication;
    }

    int grpcPort = portEnv("OZONE_BENCH_OM_GRPC_PORT");
    int timeoutMs = positiveIntEnv("OZONE_BENCH_CLUSTER_READY_TIMEOUT_MS",
        DEFAULT_READY_TIMEOUT_MS);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
        Integer.toString(grpcPort));
    conf.setBoolean(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, true);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, false);
    conf.set("ozone.server.default.replication", Integer.toString(replication));
    conf.setBoolean("ozone.tracing.enabled", boolEnv("OZONE_BENCH_OTEL",
        false));
    conf.setTimeDuration("hdds.heartbeat.interval", 1, TimeUnit.SECONDS);
    conf.setTimeDuration("ozone.scm.pipeline.creation.interval", 1,
        TimeUnit.SECONDS);

    MiniOzoneCluster cluster = null;
    try {
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(datanodes)
          .build();
      cluster.setWaitForClusterToBeReadyTimeout(timeoutMs);
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(replicationFactor(replication),
          timeoutMs);

      InetSocketAddress omRpcAddress =
          cluster.getOzoneManager().getOmRpcServerAddr();
      System.out.println(READY_PREFIX + "grpc=127.0.0.1:" + grpcPort
          + " rpc=127.0.0.1:" + omRpcAddress.getPort());
      System.out.flush();

      new BufferedReader(new InputStreamReader(System.in)).readLine();
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  private static HddsProtos.ReplicationFactor replicationFactor(
      int replication) {
    if (replication == 3) {
      return HddsProtos.ReplicationFactor.THREE;
    }
    return HddsProtos.ReplicationFactor.ONE;
  }

  private static int positiveIntEnv(String name, int defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    int parsed = Integer.parseInt(value);
    if (parsed <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return parsed;
  }

  private static boolean boolEnv(String name, boolean defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    switch (value) {
    case "1":
    case "true":
    case "TRUE":
    case "yes":
    case "YES":
      return true;
    case "0":
    case "false":
    case "FALSE":
    case "no":
    case "NO":
      return false;
    default:
      return defaultValue;
    }
  }

  private static int replicationEnv(String name, int defaultValue) {
    int replication = positiveIntEnv(name, defaultValue);
    if (replication != 1 && replication != 3) {
      throw new IllegalArgumentException(name + " must be 1 or 3");
    }
    return replication;
  }

  private static int portEnv(String name) throws Exception {
    String value = System.getenv(name);
    if (value != null && !value.trim().isEmpty()) {
      return Integer.parseInt(value);
    }
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }
}
