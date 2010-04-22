package hcache.node;

import hcache.io.CdbWriter;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Simple IPC Server that coordinates downloads across processes to prevent
 * downloading the same file 100 times simultaneously.
 */
public class HCacheNode implements HCacheNodeProtocol {
  private final Server server;
  private InetSocketAddress serverAddress;
  private static final Log LOG = LogFactory.getLog(HCacheNode.class);
  private final LocalFileStore localFiles;
  private final Configuration conf;

  static {
    Configuration.addDefaultResource("hcache-default.xml");
    Configuration.addDefaultResource("hcache-site.xml");
  }

  public HCacheNode(Configuration conf) throws IOException {
    InetSocketAddress socAddr = NetUtils.createSocketAddr(conf.get(
        "hcache.nodeAddr", "localhost:55999"));
    // create rpc server
    int handlerCount = 10;
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
        handlerCount, false, conf);
    this.serverAddress = server.getListenerAddress();
    localFiles = new LocalFileStore(conf);
    this.conf = conf;
  }

  public void start() throws IOException {
    LOG.info("HCacheNode starting on " + serverAddress);
    server.start();
    try {
      server.join();
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  public void stop() {
    server.stop();
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public static void main(String[] args) {
    int exitCode = runDaemon();
    System.exit(exitCode);
  }

  private static int runDaemon() {
    HCacheNode server = null;
    try {
      server = new HCacheNode(new Configuration());
      server.start();
      server.join();
    } catch (Exception e) {
      if (server != null)
        server.stop();
      try {
        server.join();
      } catch (InterruptedException ie) {
      }
      return 1;
    }
    return 0;
  }
  
  private HashMap<Path, Future<?>> filesUnderConstruction = new HashMap<Path, Future<?>>();
  private ExecutorService exec = Executors.newCachedThreadPool();

  /** 
   * Only service method, blocks until file is localized.
   * Guarantees that block will only be localized once.
   */
  public void localize(Path hadoopFile) {
    try {
      Future<?> transfer = null;
      // critical section, fetch future representing either 
      // already existing transfer or new transfer
      synchronized (this) {
        transfer = filesUnderConstruction.get(hadoopFile);
        if (transfer == null) {
          transfer = exec
              .submit(new FileStreamer(hadoopFile, localFiles, conf));
          filesUnderConstruction.put(hadoopFile, transfer);
        }
      }
      try {
        // wait till done, remove
        transfer.get();
        synchronized(this) {
          filesUnderConstruction.remove(hadoopFile);
        }
      } catch (ExecutionException e) {
        LOG.fatal(StringUtils.stringifyException(e));
      } catch (InterruptedException e) {
        LOG.fatal(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    } catch (IOException ioe) {
      LOG.fatal(StringUtils.stringifyException(ioe));
    }

  }

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return HCacheNodeProtocol;
  }

  /** Does work of streaming a file */
  private static class FileStreamer implements Callable<Object> {
    private final Path hadoopPath;
    private final LocalFileStore store;
    private final Configuration conf;
    private final FileSystem hadoop;

    public FileStreamer(Path hadoopPath, LocalFileStore store,
        Configuration conf) throws IOException {
      this.hadoopPath = hadoopPath;
      this.store = store;
      this.conf = conf;
      hadoop = FileSystem.get(conf);
    }
    
    @SuppressWarnings("unchecked")
    public Object call() throws Exception {
      // double check we don't already have it
      if (store.checkFile(hadoopPath) != null) return null;
      File localTmp = store.createTmp();
      CdbWriter writer = new CdbWriter(localTmp, 1024 * 16);
      KeyValueReader<Writable,Writable> reader = (KeyValueReader<Writable, Writable>) getReader(hadoopPath);
      Writable k = ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable v = ReflectionUtils.newInstance(reader.getValueClass(), conf);
      while (reader.read(k, v)) {
        writer.add(k, v);
      }
      writer.close();
      reader.close();
      store.commitLocal(hadoopPath, localTmp);
      return "success";
    }
    
    @SuppressWarnings("unchecked")
    private KeyValueReader<? extends Writable, ? extends Writable> getReader(
        Path p) throws IOException {
      FSDataInputStream in = hadoop.open(p);
      byte[] header = new byte[3];
      in.readFully(header);
      in.close();
      if (header[0] == 'S' && header[1] == 'E' && header[2] == 'Q') {
        return new SequenceReader(p, conf);
      } else {
        return new TabReader(p, conf);
      }
    }
  }
}
