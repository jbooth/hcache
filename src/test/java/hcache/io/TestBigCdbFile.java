package hcache.io;

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class TestBigCdbFile {

  @Test
  public void testCdbFile() throws Exception {
    // 100 million
    long testSize = 100000000;
    LongWritable key = new LongWritable();
    LongWritable value = new LongWritable();
    File file = new File("/tmp/cdb.test");
    OutputStream out = new BufferedOutputStream(new FileOutputStream(
        "/tmp/cdb.test"));
    BigCdbWriter<LongWritable, LongWritable> writer = new BigCdbWriter<LongWritable, LongWritable>(
        out, LongWritable.class, LongWritable.class);
    for (long j = 1; j < testSize; j++) {
      key.set(j);
      value.set(j * 10);
      if ( (j&4095l) == 0) System.out.println("Adding " + key + " : " + value);
      writer.write(key, value);
    }
    writer.close();
    CdbReader<LongWritable, LongWritable> reader = new CdbReader<LongWritable, LongWritable>(
        file, LongWritable.class, LongWritable.class);
    long start = System.nanoTime();
    for (long j = 1; j < testSize; j++) {
      if ( j % 1000 == 0) System.out.println("Searching " + j);
      key.set(j);
      value = reader.get(key);
      assertEquals(
          "Got wrong value for key " + key + ", got val : " + value.get(),
          new Long(j * 10), new Long(value.get()));
    }
    long elapsed = System.nanoTime() - start;
    System.out.println("Average Lookup time in ns : " + (elapsed / 100000));
    file.delete();
  }
}
