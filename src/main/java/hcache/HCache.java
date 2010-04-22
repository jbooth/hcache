package hcache;

import hcache.io.CdbReader;
import hcache.node.HCacheNodeProtocol;
import hcache.node.LocalFileStore;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

public class HCache {
  private final Configuration conf;
  private final HCacheNodeProtocol localNode;
  private final LocalFileStore localFiles;

  public HCache(Configuration conf) throws IOException {
    this.conf = conf;
    InetSocketAddress serverAddr = NetUtils.createSocketAddr("localhost:"
        + conf.get("hcache.nodePort", "55999"));
    localNode = (HCacheNodeProtocol) RPC.getProxy(HCacheNodeProtocol.class,
        HCacheNodeProtocol.HCacheNodeProtocol, serverAddr, conf);
    localFiles = new LocalFileStore(conf);
  }

  public <K extends Writable, V extends Writable> CdbReader<K, V> open(Path p) throws IOException {
    File f = localFiles.checkFile(p);
    if (f != null) return CdbReader.openLocal(f);
    // wait for download
    localNode.localize(p);
    f = localFiles.checkFile(p);
    if (f == null) throw new IOException("File still not present for " + p + " after namenode localization");
    return CdbReader.openLocal(f);
  }

  /**
   * Reads tab-delimited key value pairs from keyValueFile, storing them in a
   * CDB file.
   * 
   * @param keyValueFile
   * @return
   */
  public CdbReader<Text, Text> openLocalCdbFromKeyValue(Path keyValueFile) {
    return null;
  }

  /**
   * Opens a local, CDB-formatted copy of the given sequence file. If a local
   * copy already exists and is at least as new as the DFS copy, uses that,
   * otherwise streams down, formats and converts to a CDB file prior to
   * opening.
   * 
   * @param sequenceFileOnDfs
   * @return
   */
  public <K extends Writable, V extends Writable> CdbReader<K, V> openLocalCdbFromSequence(
      Path sequenceFile) {
    return null;
  }
}
