package hcache.node;

import static org.junit.Assert.assertEquals;
import hcache.HCache;
import hcache.io.CdbReader;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHCacheNode extends TestCase {
  Configuration conf;
  HCacheNode node;
  Path textFile = new Path("/tmp/hctext.txt");
  Path seqFileText = new Path("/tmp/hctext.seq");
  Path seqFileLong = new Path("/tmp/hclong.seq");

  Map<Text, Text> fileContentsText = new HashMap<Text, Text>();
  Map<LongWritable, LongWritable> fileContentsLong = new HashMap<LongWritable, LongWritable>();

  @Before
  @Override
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    this.conf = conf;
    this.node = new HCacheNode(conf);
    this.node.start();
    for (long l = 0; l < 20000; l++) {
      fileContentsText.put(new Text("" + l), new Text("" + l));
      fileContentsLong.put(new LongWritable(l), new LongWritable(l));
    }
  }
  
  @After
  @Override
  public void tearDown() throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(conf);
    fs.delete(textFile, false);
    fs.delete(seqFileText, false);
    fs.delete(seqFileLong, false);
    node.kill();
    node.join();
  }

  @Test
  public void testFileLocalize() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.delete(seqFileText, false);
    fs.delete(seqFileLong, false);
    writeSeqFile(fileContentsText, seqFileText, Text.class, Text.class);
    writeSeqFile(fileContentsLong, seqFileLong, LongWritable.class, LongWritable.class);
    
    // sequence file text
    HCache hcache = new HCache(conf);
    CdbReader<Text,Text> lookupText = hcache.open(seqFileText);
    Text valFromCdb = new Text();
    System.out.println("Checking texts");
    for (Text key : fileContentsText.keySet()) {
      lookupText.find(key, valFromCdb);
      assertEquals(fileContentsText.get(key), valFromCdb);
    }
    
    // sequence file longs
    System.out.println("Checking longs");
    CdbReader<LongWritable,LongWritable> lookupLong = hcache.open(seqFileLong);
    LongWritable val = new LongWritable();
    for (LongWritable key : fileContentsLong.keySet()) {
      lookupLong.find(key, val);
      assertEquals(fileContentsLong.get(key), val);
    }
    // text file
    fs.delete(textFile, false);
    writeTextFile(fileContentsText, textFile);
    
    CdbReader<Text,Text> lookup = hcache.open(textFile);
    for (Text key : fileContentsText.keySet()) {
      lookup.find(key, valFromCdb);
      assertEquals(fileContentsText.get(key), valFromCdb);
    }
    
  }

  public void writeTextFile(Map<Text, Text> contents, Path file) throws IOException {
    Text line = new Text();
    LineRecordWriter out = new LineRecordWriter(FileSystem.get(conf).create(file), "\t");
    for (Text key : contents.keySet()) {
      out.write(key, contents.get(key));
    }
    out.close(null);
  }

  public <K extends Writable, V extends Writable> void writeSeqFile(
      Map<K, V> contents, Path file, Class<? extends Writable> keyClass, Class<? extends Writable> valClass) throws IOException {
    SequenceFile.Writer out = new SequenceFile.Writer(FileSystem.get(conf), conf, file, keyClass, valClass);
    for (Writable k : contents.keySet()) {
      out.append(k, contents.get(k));
    }
    out.close();
  }

  protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    public LineRecordWriter(DataOutputStream out) {
      this(out, "\t");
    }

    /**
     * Write the object to the byte stream, handling Text as a special case.
     * 
     * @param o
     *          the object to print
     * @throws IOException
     *           if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
    }

    public synchronized void write(K key, V value) throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      if (!nullValue) {
        writeObject(value);
      }
      out.write(newline);
    }

    public synchronized void close(Reporter reporter) throws IOException {
      out.close();
    }
  }
}
