package hcache.io;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * Composes a CdbReader<Text,Text> and provides fast utilities for 
 * String queries by maintaining a static threadlocal Text as a buffer.
 */
public class StringReader implements Closeable {
  private static ThreadLocal<Text> keyBuff = new ThreadLocal<Text>();
  private CdbReader<Text,Text> in;
  
  public StringReader(CdbReader<Text,Text> in) {
    this.in=in;
  }
  
  public String get(String key) throws IOException {
    keyBuff.get().set(key);
    Text v = in.get(keyBuff.get());
    return v.toString();
  }
  
  public void close() throws IOException {
    in.close();
  }
}
