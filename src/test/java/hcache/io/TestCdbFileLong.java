package hcache.io;

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;


public class TestCdbFileLong {

  
  @Test
  public void testCdbFile() throws Exception {
    LongWritable key = new LongWritable();
    LongWritable value = new LongWritable();
    File file = new File("/tmp/cdb.test");
    OutputStream out = new BufferedOutputStream(new FileOutputStream("/tmp/cdb.test"));
    CdbWriter<LongWritable,LongWritable> writer = new CdbWriter<LongWritable,LongWritable>(out, LongWritable.class,LongWritable.class);
    for (long j = 1 ; j < 20 ; j++) {
      key.set(j);
      value.set(j * 1000);
      System.out.println("Adding " + key + " : " + value);
      writer.write(key, value);
    }
    writer.close();
    CdbReader<LongWritable,LongWritable> reader = new CdbReader<LongWritable,LongWritable>(file, LongWritable.class, LongWritable.class);
    long start = System.nanoTime();
    for (long j = 1 ; j < 100001 ; j++) {
      key.set(j);
      value = reader.get(key);
      System.err.println(key + " : " + value);
      assertEquals("Got wrong value for key " + key + ", got val : " + value.get(), new Long(j*1000), new Long(value.get()));
    }
    long elapsed = System.nanoTime() - start;
    System.out.println("Average Lookup time in ns : " + (elapsed/100000));
    file.delete();
  }
}
