package hcache.example;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/** Example mapper executing a map-side join */
public class ExampleMapper implements Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

}
