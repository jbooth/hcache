package hcache;

import hcache.io.CdbReader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HCache {
  private final Configuration conf;
  private final FileSystem dfs;
  private final FileSystem localfs;

  public HCache(Configuration conf) throws IOException {
    this.conf = conf;
    dfs = FileSystem.get(conf);
    localfs = FileSystem.getLocal(conf).getRaw();
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
   * copy already exists and is at least as new as the DFS copy,
   * uses that, otherwise streams down, formats and converts to a CDB file
   * prior to opening.
   * 
   * @param sequenceFileOnDfs
   * @return
   */
  public <K extends Writable, V extends Writable> CdbReader<K, V> openLocalCdbFromSequence(
      Path sequenceFile) {
    return null;
  }
}
