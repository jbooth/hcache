package hcache.io;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;


public class TestCdbFileLong {

  
  @Test
  public void testCdbFile() throws Exception {
    LongWritable key = new LongWritable();
    LongWritable value = new LongWritable();
    File file = new File("/tmp/cdb.test");
    CdbWriter writer = new CdbWriter(file, 8096);
    for (long j = 1 ; j < 100001 ; j++) {
      key.set(j);
      value.set(j * 1000);
      System.out.println("Adding " + key + " : " + value);
      writer.add(key, value);
    }
    writer.close();
    CdbReader<LongWritable,LongWritable> reader = CdbReader.openLocal(file, 2048);
    long start = System.nanoTime();
    for (long j = 1 ; j < 100001 ; j++) {
      key.set(j);
      reader.find(key, value);
      assertEquals("Got wrong value for key " + key + ", got val : " + value.get(), new Long(j*1000), new Long(value.get()));
    }
    long elapsed = System.nanoTime() - start;
    System.out.println("Average Lookup time in ns : " + (elapsed/100000));
    file.delete();
  }
}
