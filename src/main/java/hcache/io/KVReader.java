package hcache.io;

import java.io.IOException;
import java.util.List;

/**
 * Common interface for Hashfile Readers
 *
 */
public interface KVReader<K,V> {

  /** Reads 
   * @throws IOException */
  public V get(K key) throws IOException;
}
