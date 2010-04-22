package hcache.node;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;

/**
 * Key value reader for text files, splits on first tab
 */
public class TabReader implements KeyValueReader<Text,Text> {
  private LineReader in;
  private Text line = new Text();
  private static byte delim = "\t".getBytes()[0];
  
  public TabReader(Path file, Configuration conf) throws IOException {
    in = new LineReader(FileSystem.get(conf).open(file));
  }
  
  public boolean read(Text key, Text value) throws IOException {
    if (in.readLine(line) <= 0) return false;
    int delimidx = -1;
    byte[] b = line.getBytes();
    for (int i = 0 ; i < line.getLength() ; i++) {
      if (b[i]==delim) {
        delimidx=i;
        break;
      }
    }
    if (delimidx == -1) throw new IOException("Line " + line + " had no tab delimiter");
    key.set(b, 0, delim);
    value.set(b, delim, line.getLength());
    return true;
  }

  public void close() throws IOException {
    in.close();
  }

  public Class<Text> getKeyClass() {
    return Text.class;
  }

  public Class<Text> getValueClass() {
    return Text.class;
  }
}
