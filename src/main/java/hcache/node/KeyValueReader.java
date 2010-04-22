package hcache.node;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public interface KeyValueReader<K extends Writable, V extends Writable> extends Closeable {
  public boolean read(K key, V value) throws IOException;
  
  public Class<K> getKeyClass();
  public Class<V> getValueClass();
}
