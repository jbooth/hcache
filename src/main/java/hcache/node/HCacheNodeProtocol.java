package hcache.node;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface HCacheNodeProtocol extends VersionedProtocol {

  public static final long HCacheNodeProtocol = 1L;
  
  public void localizeText(Path hadoopFile);
  
  public void localizeSequence(Path hadoopFile);
  
  public void cleanup(LongWritable minAge);
}
