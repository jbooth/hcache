package hcache.node;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface HCacheNodeProtocol extends VersionedProtocol {

  public static final long HCacheNodeProtocol = 1L;
  
  public void localize(Text hadoopPath);
}
