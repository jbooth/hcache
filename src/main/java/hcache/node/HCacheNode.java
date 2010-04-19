package hcache.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Simple IPC Server that coordinates downloads across processes
 * to prevent downloading the same file 100 times simultaneously.
 */
public class HCacheNode implements HCacheNodeProtocol {
  private final Server server;
  private InetSocketAddress serverAddress;
  private static final Log LOG = LogFactory.getLog(HCacheNode.class);
  
  static {
    Configuration.addDefaultResource("hcache-default.xml");
    Configuration.addDefaultResource("hcache-site.xml");
  }
  public HCacheNode(Configuration conf) throws IOException {
    InetSocketAddress socAddr = NetUtils.createSocketAddr(conf.get("hcache.nodeAddr","localhost:55999"));
    // create rpc server 
    int handlerCount=10;
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf);
    this.serverAddress = server.getListenerAddress();
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
  
  public void stop() { server.stop(); }
  
  public static void main(String[] args) {
    try {
      HCacheNode server = new HCacheNode(new Configuration());
      server.start();
    } catch (Throwable ioe) {
      LOG.fatal(StringUtils.stringifyException(ioe));
      ioe.printStackTrace();
      System.exit(1);
    }
  }

  
  private ConcurrentHashMap<Path,Future<Boolean>> filesUnderConstruction = new ConcurrentHashMap<Path,Future<Boolean>>();
  private ExecutorService exec = Executors.newCachedThreadPool();
  
  public void cleanup(LongWritable minAge) {
    
  }

  public void localizeSequence(Path hadoopFile) {
    
    Future<Boolean> transfer=null;
    synchronized(this) {
      transfer = filesUnderConstruction.get(hadoopFile);
      if (transfer == null) {
        transfer = exec.submit(new SequenceFileStreamer());
        filesUnderConstruction.put(hadoopFile,transfer);
      }
    }
    try {
      transfer.get();
      filesUnderConstruction.remove(hadoopFile);
    } catch (ExecutionException e) {
      LOG.fatal(StringUtils.stringifyException(e));
    }
  }

  public void localizeText(Path hadoopFile) {
    
  }

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }
}
