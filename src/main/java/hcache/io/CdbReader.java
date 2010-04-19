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
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Cdb implements a Java interface to D.&nbsp;J.&nbsp;Bernstein's CDB database.
 * 
 * @author Michael Alyn Miller <malyn@strangeGizmo.com>
 * @version 1.0.3
 */
public class CdbReader<K extends Writable, V extends Writable> implements Closeable {
  private Configuration conf = new Configuration();
  private Class<K> keyClass = null;
  /** The RandomAccessFile for the CDB file. */
  private FSDataInputStream file_ = null;
  /**
   * The slot pointers, cached here for efficiency as we do not have mmap() to
   * do it for us. These entries are paired as (pos, len) tuples.
   */
  private int[] slotTable_ = null;

  /** The number of hash slots searched under this key. */
  private int loop_ = 0;

  /** The hash value for the current key. */
  private int khash_ = 0;

  /** The number of hash slots in the hash table for the current key. */
  private int hslots_ = 0;

  /** The position of the hash table for the current key */
  private int hpos_ = 0;

  /** The position of the current key in the slot. */
  private int kpos_ = 0;

  
  /** Default buffersize 2048 */
  public static <K extends Writable, V extends Writable> CdbReader<K, V> openLocal(
      File localFile) throws IOException {
    return openLocal(localFile, 2048);
  }
  
  /**
   * Opens the supplied filename on the local file system
   * 
   * @param filepath
   * @param bufferSize
   * @throws IOException
   */
  public static <K extends Writable, V extends Writable> CdbReader<K, V> openLocal(
      File localFile, int bufferSize) throws IOException {
    FileSystem localFS = FileSystem.getLocal(new Configuration()).getRaw();
    Path p = new Path(localFile.getAbsolutePath());
    return new CdbReader<K, V>(localFS.open(p, bufferSize), localFS
        .getFileStatus(p).getLen() - 2048);
  }

  /** Opens filePath on FileSystem denoted by conf */
  public CdbReader(Path filePath, int bufferSize, Configuration conf) throws IOException {
    this(FileSystem.get(conf).open(filePath, bufferSize), FileSystem.get(conf)
        .getFileStatus(filePath).getLen() - 2048);
  }

  
  /** Initializes from the given FSDataInputStream */
  @SuppressWarnings("unchecked")
  CdbReader(FSDataInputStream file, long footerStart) throws IOException {
    /* Open the CDB file. */
    file_ = file;
    /*
     * Read and parse the slot table. We do not throw an exception if this
     * fails; the file might empty, which is not an error.
     */
    /* Read the table. */
    byte[] table = new byte[2048];
    file_.seek(footerStart);
    file_.readFully(table);

    /* Create and parse the table. */
    slotTable_ = new int[256 * 2];

    int offset = 0;
    for (int i = 0; i < 256; i++) {
      int pos = table[offset++] & 0xff | ((table[offset++] & 0xff) << 8)
          | ((table[offset++] & 0xff) << 16) | ((table[offset++] & 0xff) << 24);

      int len = table[offset++] & 0xff | ((table[offset++] & 0xff) << 8)
          | ((table[offset++] & 0xff) << 16) | ((table[offset++] & 0xff) << 24);

      slotTable_[i << 1] = pos;
      slotTable_[(i << 1) + 1] = len;
    }
  }

  /**
   * Closes the CDB database.
   */
  public void close() throws IOException {
    /* Close the CDB file. */
    file_.close();
  }
//
//  /**
//   * Computes and returns the hash value for the given key.
//   * 
//   * @param key
//   *          The key to compute the hash value for.
//   * @return The hash value of <code>key</code>.
//   */
//  static final int hash(BinaryComparable bytes) {
//    byte[] key = bytes.getBytes();
//    int keyLength = bytes.getLength();
//    /* Initialize the hash value. */
//    long h = 5381;
//
//    /* Add each byte to the hash value. */
//    for (int i = 0; i < keyLength; i++) {
//      // h = ((h << 5) + h) ^ key[i];
//      long l = h << 5;
//      h += (l & 0x00000000ffffffffL);
//      h = (h & 0x00000000ffffffffL);
//
//      int k = key[i];
//      k = (k + 0x100) & 0xff;
//
//      h = h ^ k;
//    }
//
//    /* Return the hash value. */
//    return (int) (h & 0x00000000ffffffffL);
//  }

  /**
   * Prepares the class to search
   */
  public void findstart() {
    loop_ = 0;
  }

  /**
   * Finds the first record stored under the given key. Allocates and returns
   * byte[] representing value.
   * 
   * @param key
   *          The key to search for.
   * @return The record store under the given key, or <code>null</code> if no
   *         record with that key could be found.
   */
  // public byte[] find(byte[] key) throws IOException {
  // findstart();
  // int vlen = seeknext(key, key.length);
  // if (vlen == -1) return null;
  // byte[] value = new byte[vlen];
  // file_.readFully(value);
  // return value;
  // }
  /**
   * Finds the first record stored under the given key.
   * 
   * @param key
   *          The key to search for.
   * @param value
   *          Writable to fill with the result
   * @return The record store under the given key, or <code>null</code> if no
   *         record with that key could be found.
   */
  @SuppressWarnings("unchecked")
  public Writable find(K key, V value) throws IOException {
    // have to init keyclass this dumb way because generics
    // are not reified
    if (keyClass == null) {
      WritableBuffer<K> keyBuffer = new WritableBuffer<K>(8096);
      keyBuffer.put(key);
      keyClass = (Class<K>) keyBuffer.getCurrentValueClass();
    }
    findstart();
    long vpos = seeknext(key);
    if (vpos == -1)
      return null;
    value.readFields(file_);
    return value;
  }

  /**
   * Seeks to the beginning of the next record stored under the given key.
   * 
   * @param key
   *          The key to search for.
   * @param value
   *          Writable to be filled with the value
   * @return length of value if found, -1 if not found
   */
  public long seeknext(K key) throws IOException {
    K tmpKey = ReflectionUtils.newInstance(keyClass, conf);
    /* There are no keys if we could not read the slot table. */
    if (slotTable_ == null) {
      throw new IOException("Couldn't read slot table");
    }

    /* Locate the hash entry if we have not yet done so. */
    if (loop_ == 0) {
      /* Get the hash value for the key. */
      //int u = hash(keyBytes, keyLength);
      int u = key.hashCode();
      /* Unpack the information for this record. */
      int slot = u & 255;
      hslots_ = slotTable_[(slot << 1) + 1];
      if (hslots_ == 0) {
        return -1;
      }
      hpos_ = slotTable_[slot << 1];

      /* Store the hash value. */
      khash_ = u;

      /* Locate the slot containing this key. */
      u >>>= 8;
      u %= hslots_;
      u <<= 3;
      kpos_ = hpos_ + u;
    }

    /* Search all of the hash slots for this key. */

    try {
      while (loop_ < hslots_) {
        /* Read the entry for this key from the hash slot. */
        file_.seek(kpos_);

        int h = file_.readUnsignedByte() | (file_.readUnsignedByte() << 8)
            | (file_.readUnsignedByte() << 16)
            | (file_.readUnsignedByte() << 24);

        int pos = file_.readUnsignedByte() | (file_.readUnsignedByte() << 8)
            | (file_.readUnsignedByte() << 16)
            | (file_.readUnsignedByte() << 24);
        if (pos == 0) {
          // key not found
          return -1;
        }

        /*
         * Advance the loop count and key position. Wrap the key position around
         * to the beginning of the hash slot if we are at the end of the table.
         */
        loop_ += 1;

        kpos_ += 8;
        if (kpos_ == (hpos_ + (hslots_ << 3)))
          kpos_ = hpos_;

        /* Ignore this entry if the hash values do not match. */
        if (h != khash_)
          continue;

        /*
         * Get the length of the key and data in this hash slot entry.
         */
        file_.seek(pos);

        /*
         * Read the key stored in this entry and compare it to the key we were
         * given.
         */
        tmpKey.readFields(file_);
        boolean match = (key.equals(tmpKey));

        /* No match; check the next slot. */
        if (!match)
          continue;

        /* The keys match, return value length */
        return file_.getPos();

        // byte[] d = new byte[dlen];
        // file_.readFully(d);
        // return d;
      }
    } catch (IOException ignored) {
      return -1;
    }

    /* No more data values for this key. */
    return -1;
  }

  /**
   * Returns an Enumeration containing a CdbElement for each entry in the
   * constant database.
   * 
   * @param filepath
   *          The CDB file to read.
   * @return An Enumeration containing a CdbElement for each entry in the
   *         constant database.
   * @exception java.io.IOException
   *              if an error occurs reading the constant database.
   */
  public static Enumeration elements(final String filepath) throws IOException {
    /* Open the data file. */
    final InputStream in = new BufferedInputStream(
        new FileInputStream(filepath));

    /* Read the end-of-data value. */
    final int eod = (in.read() & 0xff) | ((in.read() & 0xff) << 8)
        | ((in.read() & 0xff) << 16) | ((in.read() & 0xff) << 24);

    /* Skip the rest of the hashtable. */
    in.skip(2048 - 4);

    /* Return the Enumeration. */
    return new Enumeration() {
      /* Current data pointer. */
      int pos = 2048;

      /* Finalizer. */
      protected void finalize() {
        try {
          in.close();
        } catch (Exception ignored) {
        }
      }

      /*
       * Returns <code>true</code> if there are more elements in the constant
       * database (pos < eod); <code>false</code> otherwise.
       */
      public boolean hasMoreElements() {
        return pos < eod;
      }

      /* Returns the next data element in the CDB file. */
      public synchronized Object nextElement() {
        try {
          /* Read the key and value lengths. */
          int klen = readLeInt();
          pos += 4;
          int dlen = readLeInt();
          pos += 4;

          /* Read the key. */
          byte[] key = new byte[klen];
          for (int off = 0; off < klen; /* below */) {
            int count = in.read(key, off, klen - off);
            if (count == -1)
              throw new IllegalArgumentException("invalid cdb format");
            off += count;
          }
          pos += klen;

          /* Read the data. */
          byte[] data = new byte[dlen];
          for (int off = 0; off < dlen; /* below */) {
            int count = in.read(data, off, dlen - off);
            if (count == -1)
              throw new IllegalArgumentException("invalid cdb format");
            off += count;
          }
          pos += dlen;

          /* Return a CdbElement with the key and data. */
          return new CdbElement(key, data);
        } catch (IOException ioException) {
          throw new IllegalArgumentException("invalid cdb format");
        }
      }

      /* Reads a little-endian integer from <code>in</code>. */
      private int readLeInt() throws IOException {
        return (in.read() & 0xff) | ((in.read() & 0xff) << 8)
            | ((in.read() & 0xff) << 16) | ((in.read() & 0xff) << 24);
      }
    };
  }

  private static class LocalFSInput extends FSInputStream {
    private final FileInputStream in;

    public LocalFSInput(String filepath) throws IOException {
      in = new FileInputStream(filepath);
    }

    public int read() throws IOException {
      return in.read();
    }

    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    public void close() throws IOException {
      in.close();
    }

    public void seek(long pos) throws IOException {
      in.getChannel().position(pos);
    }

    public long getPos() throws IOException {
      return in.getChannel().position();
    }

    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }
}
