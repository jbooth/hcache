package hcache.node;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class SequenceReader implements KeyValueReader {
  private SequenceFile.Reader in;

  public SequenceReader(Path file, Configuration conf) throws IOException {
    in = new SequenceFile.Reader(FileSystem.get(conf), file, conf);
  }
  
  public boolean read(Writable key, Writable value) throws IOException {
    return in.next(key, value);
  }
  
  public void close() throws IOException {
    in.close();
  }

  public Class getKeyClass() {
    return in.getKeyClass();
  }

  public Class getValueClass() {
    return in.getValueClass();
  }
}
