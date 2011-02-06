package hcache.io;

import hcache.io.CdbWriter.CdbHashPointer;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Cuts memory usage by 1/256 by writing hash buckets to disk instead of
 * aggregating in memory, then reading/writing one set of buckets at a time to
 * complete the file.
 * 
 * @author jay
 * 
 * @param <K>
 * @param <V>
 */
public class BigCdbWriter<K extends Writable, V extends Writable> implements
    Closeable {
  // number of slots in the slot table
  // should take the form of 2^x
  public static final int SLOTSIZE = 4096;
  // all 1's, so (n & MAGIC == n % SLOTSIZE)
  public static final int MAGIC = 4095;
  // TODO make SLOTSIZE and MAGIC

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final FSDataOutputStream out;

  /*
   * New code: we have 256 slotfiles, and keep a count of how many total entries
   * are in each. later, we reread
   */
  private static final int SLOTFILES = 256;
  private static final int SLOTFMAGIC = 255;
  private static final File tempDir = new File(System.getProperty("cdb.tmpdir",
      "/tmp"));
  private final UUID id;
  private final FSDataOutputStream[] slotsOut = new FSDataOutputStream[SLOTFILES];
  private final File[] slotFiles = new File[SLOTFILES];
  private final int[] entriesPerFile = new int[SLOTFILES];

  /** The number of entries in each hash table. */
  private final int[] tableCount;

  public BigCdbWriter(OutputStream out, Class<K> keyClass, Class<V> valueClass)
      throws IOException {
    this(new FSDataOutputStream(out, null), keyClass, valueClass);
  }

  public BigCdbWriter(FSDataOutputStream out, Class<K> keyClass,
      Class<V> valueClass) throws IOException {
    this.out = out;
    // todo need SLOTSIZE and MAGIC
    this.tableCount = new int[SLOTSIZE];
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    /* Records can't be at position zero, so write one filler byte */
    out.write((byte) -1);
    // initialize slotfiles
    this.id = UUID.randomUUID();
    for (int i = 0; i < SLOTFILES; i++) {
      File slotFile = new File(tempDir, id.toString() + "-" + i);
      slotFiles[i] = slotFile;
      FSDataOutputStream slotOut = new FSDataOutputStream(
          (OutputStream)
          new BufferedOutputStream(
              new FileOutputStream(slotFile)
              ),null);
      slotsOut[i] = slotOut;
    }
  }

  public void write(K key, V value) throws IOException {
    long recordPos = out.getPos();
    /* Write out the key and data . */
    key.write(out);
    value.write(out);
    /* Add the hash pointer to our list. */
    int hash = hash(key);
    /* Add this item to the count. */
    tableCount[(hash & MAGIC)]++;
    
    // Write to it's slotfile.. this will be re-read and consolidated
    // upon close()
    int slotIdx = hash&SLOTFMAGIC;
    entriesPerFile[slotIdx]++;
    new CdbHashPointer(hash, recordPos).write(slotsOut[slotIdx]);
    
  }

  public void close() throws IOException {
    // flush outfiles
    for (OutputStream out : slotsOut) {
      out.close();
    }
    // read each slotfile and write its contents, keeping track
    // of slot positions as we go
    long[] slotPos = new long[SLOTSIZE];
    int[] slotLen = new int[SLOTSIZE];
    for (int i = 0 ; i < SLOTFILES ; i++) {
      System.err.println("file " + i);
      int numEntries = entriesPerFile[i];
      SeekableInputStream in = 
        new SeekableInputStream(
            new RandomAccessFile(slotFiles[i],"r").getChannel(), 
            0,
            ByteBuffer.allocate(8192));
      Multimap<Integer,CdbHashPointer> slots = HashMultimap.create();
      long fileLength = slotFiles[i].length();
      for (int j = 0 ; j < numEntries ; j++) {
        CdbHashPointer row = new CdbHashPointer(0,0);
        row.readFields(in);
        slots.put(row.hash&MAGIC, row);
      }
      // read all, close and delete file
      in.close();
      slotFiles[i].delete();
      for ( int slot : slots.keySet()) {
        long pos = out.getPos();
        int len = tableCount[slot];
        // update footer values
        slotPos[slot] = pos;
        slotLen[slot] = len;
        // build/write the slot table
        CdbHashPointer[] hashTable = new CdbHashPointer[len];
        for (CdbHashPointer hp : slots.get(slot)) {
          /* Locate a free space in the hash table. */
          int where = (hp.hash >>> 8) % len;
          while (hashTable[where] != null)
            if (++where == len)
              where = 0;
          /* Store the hash pointer. */
          hashTable[where] = hp;
        }
        // ok now write it
        for (int j = 0 ; j < len ; j++) {
          hashTable[j].write(out);
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

}
