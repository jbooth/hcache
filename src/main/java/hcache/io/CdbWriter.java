package hcache.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Writable;

public class CdbWriter<K extends Writable, V extends Writable> implements
    Closeable {
  // number of slots in the slot table
  // should take the form of 2^x-1, so we can use & for quick modulo
  public static final int SLOTSIZE = 4095;

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final FSDataOutputStream out;

  /**
   * The list of hash pointers in the file, in their order in the constant
   * database.
   */
  private final List<CdbHashPointer> hashPointers;

  /** The number of entries in each hash table. */
  private final int[] tableCount;

  public CdbWriter(OutputStream out, Class<K> keyClass, Class<V> valueClass)
      throws IOException {
    this(new FSDataOutputStream(out, null), keyClass, valueClass);
  }

  public CdbWriter(FSDataOutputStream out, Class<K> keyClass,
      Class<V> valueClass) throws IOException {
    this.out = out;
    this.tableCount = new int[SLOTSIZE];
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.hashPointers = new ArrayList<CdbHashPointer>();
    /* Clear the table counts. */
    for (int i = 0; i < 256; i++)
      tableCount[i] = 0;
    /* Records can't be at position zero, so write one filler byte */
    out.write((byte) -1);
  }

  public void write(K key, V value) throws IOException {
    long recordPos = out.getPos();
    /* Write out the key and data . */
    key.write(out);
    value.write(out);
    int recordLen = (int) (out.getPos() - recordPos);

    /* Add the hash pointer to our list. */
    int hash = hash(key);
    hashPointers.add(new CdbHashPointer(hash, recordPos));
    System.err.println("Wrote " + key + ", pos : " + recordPos);
    /* Add this item to the count. */
    tableCount[hash & (SLOTSIZE - 1)]++;
  }

  public void close() throws IOException {
    // write slot sections, then slot index
    /* Find the start of each hash table. */
    int curEntry = 0;

    // sort hash entries so they'll be adjacent to equal hashes
    Collections.sort(hashPointers);
    // store the index for the start of each slotsize in the list
    int[] tableStart = new int[SLOTSIZE];
    CdbHashPointer last = null;
    for (int i = 0 ; i < hashPointers.size() ; i++) {
      CdbHashPointer hp = hashPointers.get(i);
      if (last == null || hp.hash != last.hash) {
        tableStart[hp.hash & SLOTSIZE] = i;
      }
      last = hp;
    }
    // now write the hash entries, build slot table as we go
    long[] slotPos = new long[SLOTSIZE];
    int[] slotLen = new int[SLOTSIZE];
    
    
    for (int i = 0; i < SLOTSIZE; i++) {
      int len = tableCount[i];
      // length 0 means we're a nonentity
      if (len == 0) {
        slotPos[i] = 0;
        slotLen[i] = 0;
        continue;
      }
      // record that we have a slot here
      slotPos[i] = out.getPos();
      slotLen[i] = len;
      
      /* Build the hash table for this slot. */
      int start = tableStart[i];
      CdbHashPointer[] hashTable = new CdbHashPointer[len];
      for (int u = 0; u < len; u++) {
        /* Get the hash pointer. */
        CdbHashPointer hp = hashPointers.get(u + start);

        /* Locate a free space in the hash table. */
        int where = (hp.hash >>> 8) % len;
        while (hashTable[where] != null)
          if (++where == len)
            where = 0;
        System.err.println("Storing " + hp + " at " + where);
        /* Store the hash pointer. */
        hashTable[where] = hp;
      }
      
      
      
      /* Write out the hash table. */
      for (int u = 0; u < len; u++) {
        CdbHashPointer hp = hashTable[u];
        if (hp != null) {
          System.err.println(hashTable[u] + " at " + out.getPos());
          out.writeInt(hashTable[u].hash);
          out.writeLong(hashTable[u].pos);
        } else {
          out.writeInt(0);
          out.writeLong(0);
        }
      }
    }
    /* Append the slot table as the last few kb of file. */
    for (int i = 0; i < SLOTSIZE; i++) {
      out.writeLong(slotPos[i]);
      out.writeInt(slotLen[i]);
    }

    /* Close the file. */
    out.close();
  }

  // same as HashPartitioner
  static int hash(Writable h) {
    return h.hashCode() & Integer.MAX_VALUE;
  }

  static class CdbHashPointer implements Comparable<CdbHashPointer> {
    /** The hash value of this entry. */
    final int hash;

    /** The position in the constant database of this entry. */
    final long pos;

    // natural sort order by hash
    public int compareTo(CdbHashPointer o) {
      int thisVal = this.hash;
      int anotherVal = o.hash;
      return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
    };

    /**
     * Creates a new CdbHashPointer and initializes it with the given hash value
     * and position.
     * 
     * @param hash
     *          The hash value for this hash pointer.
     * @param pos
     *          The position of this entry in the constant database.
     */
    CdbHashPointer(int hash, long pos) {
      this.hash = hash;
      this.pos = pos;
    }

    @Override
    public String toString() {
      return "CdbHashPointer [hash=" + hash + ", pos=" + pos + "]";
    }

  }
}
