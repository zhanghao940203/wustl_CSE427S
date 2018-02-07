package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * TODO implement
     */
	  String line = value.toString();
	  String[] aline = line.split(" ");
	  for(int i=0;i<aline.length-1;i++){
		  aline[i] = aline[i].toLowerCase();
		  aline[i+1] = aline[i+1].toLowerCase();
		  context.write(new Text(aline[i]+", "+aline[i+1]), new IntWritable(1));
	  }
  }
}
