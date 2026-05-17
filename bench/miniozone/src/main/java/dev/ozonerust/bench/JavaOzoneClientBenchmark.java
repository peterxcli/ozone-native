package dev.ozonerust.bench;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

public final class JavaOzoneClientBenchmark implements AutoCloseable {
  private static final String STREAM_READ_BLOCK_KEY =
      "ozone.client.stream.readblock.enable";
  private static final String STREAM_READ_BLOCK_ENV =
      "OZONE_BENCH_JAVA_STREAM_READBLOCK_ENABLE";
  private static final String READY_LINE = "Ready!";
  private static final String MEASURED_PREFIX = "Measured! ";
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final String READ_KEY = "bench-read";
  private static final String WRITE_KEY = "bench-write";
  private static final String RPC_KEY = "bench-rpc-dir";

  private final OzoneClient client;
  private final ObjectStore store;
  private final OzoneVolume volume;
  private final OzoneBucket bucket;
  private final String volumeName;
  private final String bucketName;
  private final byte[] data;
  private final byte[] readBuffer = new byte[BUFFER_SIZE];
  private final RatisReplicationConfig replicationConfig;
  private final ExecutorService executor;
  private final int parallelism;
  private volatile long blackhole;

  private JavaOzoneClientBenchmark(String endpoint, int fileMib,
      int parallelism, int replication) throws Exception {
    Endpoint parsed = Endpoint.parse(endpoint);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("hdds.block.token.enabled", false);
    conf.set("ozone.server.default.replication", Integer.toString(replication));
    conf.setBoolean(STREAM_READ_BLOCK_KEY, booleanEnv(STREAM_READ_BLOCK_ENV,
        true));

    this.replicationConfig =
        RatisReplicationConfig.getInstance(replicationFactor(replication));
    this.client = OzoneClientFactory.getRpcClient(parsed.host, parsed.port,
        conf);
    this.store = client.getObjectStore();
    this.volumeName = "bench-java-" + UUID.randomUUID().toString()
        .replace("-", "");
    this.bucketName = "bucket-" + UUID.randomUUID().toString()
        .replace("-", "");
    this.data = benchData(fileMib);
    this.executor = Executors.newFixedThreadPool(parallelism);
    this.parallelism = parallelism;

    store.createVolume(volumeName);
    this.volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    this.bucket = volume.getBucket(bucketName);

    writeFile(READ_KEY);
    bucket.createDirectory(RPC_KEY);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException("Usage: "
          + JavaOzoneClientBenchmark.class.getName()
          + " <om-rpc-host:port> <file-mib> <parallelism> <replication>");
    }

    String endpoint = args[0];
    int fileMib = positiveInt(args[1], "file-mib");
    int parallelism = positiveInt(args[2], "parallelism");
    int replication = replication(args[3]);

    try (JavaOzoneClientBenchmark benchmark =
             new JavaOzoneClientBenchmark(endpoint, fileMib, parallelism,
                 replication);
         BufferedReader stdin = new BufferedReader(
             new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      System.out.println(READY_LINE);
      System.out.flush();
      benchmark.commandLoop(stdin);
    }
  }

  private void commandLoop(BufferedReader stdin) throws IOException {
    String line;
    while ((line = stdin.readLine()) != null) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      if ("close".equals(trimmed)) {
        return;
      }
      String[] parts = trimmed.split("\\s+");
      if (parts.length != 3 || !"measure".equals(parts[0])) {
        System.out.println("Error! unsupported command: " + trimmed);
        System.out.flush();
        continue;
      }

      try {
        long iterations = Long.parseLong(parts[2]);
        long nanos = measure(parts[1], iterations);
        System.out.println(MEASURED_PREFIX + nanos);
      } catch (Exception ex) {
        ex.printStackTrace(System.err);
        System.out.println("Error! " + ex.getClass().getName() + ": "
            + ex.getMessage());
      }
      System.out.flush();
    }
  }

  private long measure(String operation, long iterations) throws Exception {
    long started = System.nanoTime();
    for (long i = 0; i < iterations; i++) {
      if ("read".equals(operation)) {
        blackhole += readFile();
      } else if ("write".equals(operation)) {
        writeFile(WRITE_KEY);
      } else if ("getFileStatus".equals(operation)) {
        blackhole += bucket.getFileStatus(RPC_KEY).isDirectory() ? 1 : 2;
      } else if ("getFileStatusParallel".equals(operation)) {
        blackhole += getFileStatusParallel();
      } else {
        throw new IllegalArgumentException("unsupported operation: "
            + operation);
      }
    }
    long elapsed = System.nanoTime() - started;
    // Prevent JIT from optimizing away the blackhole updates.
    if (blackhole == Long.MIN_VALUE) {
      System.err.println("blackhole=" + blackhole);
    }
    return elapsed;
  }

  private int readFile() throws IOException {
    int total = 0;
    try (OzoneInputStream in = bucket.readFile(READ_KEY)) {
      int read;
      while ((read = in.read(readBuffer)) != -1) {
        total += read;
      }
    }
    return total;
  }

  private void writeFile(String key) throws IOException {
    try (OzoneOutputStream out = bucket.createFile(key, data.length,
        replicationConfig, true, true)) {
      out.write(data, 0, data.length);
    }
  }

  private long getFileStatusParallel() throws InterruptedException,
      ExecutionException {
    List<Future<Boolean>> futures = new ArrayList<>();
    for (int i = 0; i < parallelism; i++) {
      futures.add(executor.submit(() -> bucket.getFileStatus(RPC_KEY)
          .isDirectory()));
    }

    long observed = 0;
    for (Future<Boolean> future : futures) {
      observed += future.get() ? 1 : 2;
    }
    return observed;
  }

  @Override
  public void close() throws Exception {
    executor.shutdownNow();
    try {
      bucket.deleteKey(READ_KEY);
    } catch (Exception ignored) {
    }
    try {
      bucket.deleteKey(WRITE_KEY);
    } catch (Exception ignored) {
    }
    try {
      bucket.deleteDirectory(RPC_KEY, true);
    } catch (Exception ignored) {
    }
    try {
      volume.deleteBucket(bucketName);
    } catch (Exception ignored) {
    }
    try {
      store.deleteVolume(volumeName);
    } catch (Exception ignored) {
    }
    client.close();
  }

  private static byte[] benchData(int fileMib) {
    int byteLength = fileMib * 1024 * 1024;
    byte[] bytes = new byte[byteLength];
    for (int i = 0; i < byteLength; i++) {
      bytes[i] = (byte) i;
    }
    return bytes;
  }

  private static ReplicationFactor replicationFactor(int replication) {
    if (replication == 3) {
      return HddsProtos.ReplicationFactor.THREE;
    }
    return HddsProtos.ReplicationFactor.ONE;
  }

  private static int positiveInt(String value, String name) {
    int parsed = Integer.parseInt(value);
    if (parsed <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return parsed;
  }

  private static int replication(String value) {
    int parsed = positiveInt(value, "replication");
    if (parsed != 1 && parsed != 3) {
      throw new IllegalArgumentException("replication must be 1 or 3");
    }
    return parsed;
  }

  private static boolean booleanEnv(String name, boolean defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    String normalized = value.trim().toLowerCase();
    if ("1".equals(normalized) || "true".equals(normalized)
        || "yes".equals(normalized)) {
      return true;
    }
    if ("0".equals(normalized) || "false".equals(normalized)
        || "no".equals(normalized)) {
      return false;
    }
    throw new IllegalArgumentException(name + " must be boolean");
  }

  private static final class Endpoint {
    private final String host;
    private final int port;

    private Endpoint(String host, int port) {
      this.host = host;
      this.port = port;
    }

    private static Endpoint parse(String value) {
      int separator = value.lastIndexOf(':');
      if (separator <= 0 || separator == value.length() - 1) {
        throw new IllegalArgumentException("endpoint must be host:port");
      }
      String host = value.substring(0, separator);
      int port = Integer.parseInt(value.substring(separator + 1));
      return new Endpoint(host, port);
    }
  }
}
