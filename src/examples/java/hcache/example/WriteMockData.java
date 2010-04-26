package hcache.example;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

// unfinished! 

public class WriteMockData {
  
  
  
  
  public static void main(String[] args) {
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
  
  
  /**
   * Writes a random stream of click data with the given user IDs and 
   * pages to be clicked on
   * 
   * @param out
   * @param consumerIds
   * @param pages
   * @param numRows
   */
  private static void writeClickStream(Configuration conf, Path p, List<Integer> consumerIds, List<String> pages, int numRows) {
    Random r = new Random();
    
  }

  /**
   * Writes a full dimension of gender data as a Sequence file
   * 
   * @param out
   */
  private static void writeUserGender(Configuration conf, Path p, Collection<Integer> consumerIDs) throws IOException {
    Random r = new Random();
    SequenceFile.Writer out = new SequenceFile.Writer(FileSystem.get(conf), conf, p, IntWritable.class, Text.class);
    IntWritable key = new IntWritable();
    Text MALE = new Text("MALE");
    Text FEMALE = new Text("FEMALE");
    for (int consumerId : consumerIDs) {
      key.set(consumerId);
      if (r.nextBoolean()) {
        out.append(key, MALE);
      } else {
        out.append(key, FEMALE);
      }
    }
    out.close();
  }
  
  
}
