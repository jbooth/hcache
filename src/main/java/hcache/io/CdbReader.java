/*
 * Copyright (c) 2000-2001, Michael Alyn Miller <malyn@strangeGizmo.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of Michael Alyn Miller nor the names of the
 *    contributors to this software may be used to endorse or promote
 *    products derived from this software without specific prior written
 *    permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package hcache.io;

/* Java imports. */
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Cdb implements a Java interface to D.&nbsp;J.&nbsp;Bernstein's CDB database.
 */
public class CdbReader<K extends Writable, V extends Writable> implements
    Closeable,KVReader<K,V> {
  private Configuration conf = new Configuration();
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  /** The RandomAccessFile for the CDB file. */
  private final FileChannel file_;
  private final SeekableInputStream in;
  
  /**
   * The slot pointers, cached here for efficiency as we do not have mmap() to
   * do it for us. These entries are paired as (pos, len) tuples.
   */
  private final long[] slotPos;
  private final int[] slotLen;

  public CdbReader(File file, Class<K> keyClass, Class<V> valueClass) throws IOException {
    this(file,4096,keyClass,valueClass);
  }
  public CdbReader(File file, int bufferSize, Class<K> keyClass, Class<V> valueClass) throws IOException {
    this(new RandomAccessFile(file,"r").getChannel(), bufferSize, keyClass, valueClass);
  }
  
  /** Initializes from the given FSDataInputStream */
  @SuppressWarnings("unchecked")
  public CdbReader(FileChannel file, int bufferSize, Class<K> keyClass, Class<V> valueClass) throws IOException {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    long footerStart = file.size() - (12 * CdbWriter.SLOTSIZE);
    /* Open the CDB file. */
    file_ = file;
    /*
     * Read and parse the slot table. We do not throw an exception if this
     * fails; the file might empty, which is not an error.
     */
    /* Read the table. */
    in = new SeekableInputStream(file_, footerStart,
        ByteBuffer.allocate(bufferSize));
    slotPos = new long[CdbWriter.SLOTSIZE];
    slotLen = new int[CdbWriter.SLOTSIZE];
    for (int i = 0 ; i < CdbWriter.SLOTSIZE ; i++) {
      slotPos[i] = in.readLong();
      slotLen[i] = in.readInt();
    }
  }

  /**
   * Closes the CDB database.
   */
  public void close() throws IOException {
    /* Close the CDB file. */
    file_.close();
  }
  
  /**
   * Gets the values for the given key.
   * 
   * @param key
   *          The key to search for.\
   * @return length of value if found, -1 if not found
   */
  public V get(K key) throws IOException {
    K tmpKey = ReflectionUtils.newInstance(keyClass, conf);
    int hash = CdbWriter.hash(key); 

    /* Locate the hash slot we're in */
    int slot = hash & CdbWriter.MAGIC;
    int hlen = slotLen[slot];
    long hpos = slotPos[slot];
    // if no entries for this slot, just bail
    if (hlen == 0) {
      return null;
    }

    /* Now seek to slot containing this key */
    in.seek(hpos);
    // find offset in hash by magic from CdbWriter
    int offset = (hash >>> 8) % hlen;
    // seek to approximate offset
    in.seek(hpos + (offset*12));
    
    for (int i = 0 ; i < hlen ; i++) {
      if (in.getPos() == (hpos + (hlen*12))) {
        // we missed and need to wrap around to beginning of hash
        in.seek(hpos);
      }
      int h = in.readInt();
      long kpos = in.readLong();
      // if this is our match
      if (h == hash) {
        long prevPos = in.getPos();
        // check for key
        in.seek(kpos);
        tmpKey.readFields(in);
        if(tmpKey.equals(key)) {
          // found our match!
          V ret;
          try {
            ret = valueClass.newInstance();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          ret.readFields(in);
          return ret;
        } else {
          // key wasn't equal, continue reading hash section from previous position
          in.seek(prevPos);
        }
      }
    }
    /* No more data values for this key. */
    return null;
  }
}
