package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProcessLogs extends Configured implements Tool{

  public static void main(String[] args) throws Exception {
	int exitCode = ToolRunner.run(new Configuration(), new ProcessLogs(), args);
	System.exit(exitCode);
  }
  public int run(String[] args) throws Exception{
	  if (args.length != 2) {
	      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
	      return -1;
	    }
	
	    Job job = new Job(getConf());
	    job.setJarByClass(ProcessLogs.class);
	    job.setJobName("Process Logs");
	
	    /*
	     * TODO implement
	     */
	
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    job.setMapperClass(LogFileMapper.class);
	    job.setReducerClass(LogFileReducer.class);
	
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
  }
}
