//package hcache.io;
//
//import static org.junit.Assert.assertEquals;
//
//import java.io.File;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Random;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.junit.Test;
//
//public class TestCdbFileBytes {
//  @Test
//  public void testCdbFile() throws Exception {
//    Random r = new Random(0xCAFEBABE);
//    Map<Text,Text> keyValue = new HashMap<Text,Text>();
//    File file = new File("/tmp/cdb.test");
//    CdbWriter<Text,Text> writer = new CdbWriter<Text,Text>(file, 8096);
//    for (long j = 0 ; j < 10000 ; j++) {
//      byte[] k = new byte[256];
//      r.nextBytes(k);
//      byte[] v = new byte[512];
//      r.nextBytes(v);
//      Text key = new Text(k);
//      Text value = new Text(v);
//      //System.out.println("Adding " + key + " : " + value);
//      keyValue.put(key, value);
//      writer.add(key, value);
//    }
//    writer.close();
//    CdbReader<Text,Text> reader = CdbReader.openLocal(file, 2048);
//    long start = System.nanoTime();
//    Text key = new Text();
//    Text value = new Text();
//    for (Text k : keyValue.keySet()) {
//      reader.find(k, value);
//      assertEquals("Got wrong value for key " + key + ", got val : " + value, keyValue.get(k), value);
//    }
//    long elapsed = System.nanoTime() - start;
//    System.out.println("Average Lookup time in ns : " + (elapsed/100000));
//    file.delete();
//  }
//}
