package hcache.node;

import hcache.io.CdbWriter;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Class to stream files from DFS to 
 */
public abstract class CdbStreamer<K extends Writable,V extends Writable> implements Callable<Boolean> {
  private Configuration conf;
  private File tmpLocalFile;
  private Class<? extends K> keyClass;
  private Class<? extends V> valClass;
  
  public CdbStreamer(Configuration conf, Path hadoopFile, File tmpLocalFile, Class<? extends K> keyClass, Class<? extends V> valClass) {
    this.conf=conf;
    this.tmpLocalFile=tmpLocalFile;
    this.keyClass=keyClass;
    this.valClass=valClass;
  }
//  
//  @Override
//  public Boolean call() { stream(); return true; }
  
  /** Streams key/value pairs out of HadoopFile on the filesystem denoted by conf
   * to File tmpLocalFile.  Used by LocalFileStore, which then moves tmpLocalFile
   * into place.  
   * @throws IOException */
  public void stream() throws IOException {
    K key = ReflectionUtils.newInstance(keyClass, conf);
    V value = ReflectionUtils.newInstance(valClass, conf);
    CdbWriter<K,V> writer = new CdbWriter<K,V>(tmpLocalFile, 1024*32);
    while (next(key,value)) {
      writer.add(key, value);
    }
    writer.close();
  }
  
  
  /** Fills key and value with the next pair.  Returns false at eof 
   * @throws IOException */
  public abstract boolean next(K key, V value) throws IOException;
  
  /** Converts a SequenceFile to a local Cdb. */
  @SuppressWarnings("unchecked")
  public static <K extends Writable, V extends Writable> CdbStreamer<K,V>  sequenceFileStreamer(Configuration conf, Path hadoopFile, File tmpLocalFile) throws IOException {
    final SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), hadoopFile, conf);
    
    return new CdbStreamer<K,V>(conf, 
        hadoopFile, tmpLocalFile, 
        (Class<? extends K>)reader.getKeyClass(), 
        (Class<? extends V>)reader.getValueClass()) {
      @Override
      public boolean next(K key, V value) throws IOException { 
        return reader.next(key,value);
      }
    };
  }

  /** Converts a text file of tab-separated keys and values to a local CDB */
  public static CdbStreamer<Text,Text> keyValueStreamer(Configuration conf, Path hadoopFile, File tmpLocalFile) throws IOException {
    final Text t = new Text();
    final LineReader lr = new LineReader(FileSystem.get(conf).open(hadoopFile));
    final byte delim = "\t".getBytes()[0];
    
    return new CdbStreamer<Text,Text>(conf,hadoopFile,tmpLocalFile,Text.class,Text.class) {
      @Override
      public boolean next(Text key, Text value) throws IOException {
        int lastRead = lr.readLine(t, 16*1024);
        if (lastRead <= 0) return false;
        byte[] b = t.getBytes();
        int delimPos=-1;
        for (int i = 0 ; i < t.getLength() ; i++) {
          if (b[i] == delim) {
            delimPos = i;
            break;
          }
        }
        if (delimPos == -1) throw new IOException("No tab in line : " + t.toString());
        key.set(b, 0, delimPos);
        value.set(b, delimPos, (t.getLength() - delimPos));
        return true;
      }
    };
  }
}
