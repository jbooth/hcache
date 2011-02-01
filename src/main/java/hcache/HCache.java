//package hcache;
//
//import hcache.io.CdbReader;
//import hcache.node.HCacheNodeProtocol;
//import hcache.node.LocalFileStore;
//
//import java.io.File;
//import java.io.IOException;
//import java.net.InetSocketAddress;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.ipc.RPC;
//import org.apache.hadoop.net.NetUtils;
//import org.mortbay.log.Log;
//
//public class HCache {
//  private final Configuration conf;
//  private final HCacheNodeProtocol localNode;
//  private final LocalFileStore localFiles;
//
//  /**
//   * Constructs an hcache client, connecting to the localization service
//   * on localhost.
//   * 
//   * @param conf
//   * @throws IOException
//   */
//  public HCache(Configuration conf) throws IOException {
//    this.conf = conf;
//    InetSocketAddress serverAddr = NetUtils.createSocketAddr("localhost:"
//        + conf.get("hcache.nodePort", "55999"));
//    Log.info("Initializing hcache client");
//    localNode = (HCacheNodeProtocol) RPC.getProxy(HCacheNodeProtocol.class,
//        HCacheNodeProtocol.HCacheNodeProtocol, serverAddr, conf);
//    localFiles = new LocalFileStore(conf);
//  }
//
//  /**
//   * Attempts to open the local key-value store for the file denoted by
//   * Path p.  If the local key-value store is nonexistent or out of date,
//   * we synchronize on the localization service until it's been pulled down.
//   * 
//   */
//  public <K extends Writable, V extends Writable> CdbReader<K, V> open(Path p) throws IOException {
//    File f = localFiles.checkFile(p);
//    if (f != null) return CdbReader.openLocal(f);
//    // wait for download
//    Log.info(p + " is not localized, localizing..");
//    localNode.localize(new Text(p.toString()));
//    f = localFiles.checkFile(p);
//    if (f == null) throw new IOException("File still not present for " + p + " after namenode localization");
//    return CdbReader.openLocal(f);
//  }
//}
