package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreateSequenceFileParameter extends Configured implements Tool {
	private boolean compress = false;
	 public int run(String[] args) throws Exception {

		    if (args.length != 2) {
		      System.out.printf("Usage: CreateSequenceFile <input dir> <output dir>\n");
		      return -1;
		    }

		    Job job = new Job(getConf());
		    job.setJarByClass(CreateSequenceFileParameter.class);
		    job.setJobName("Create Sequence File");

		    /*
		     * TODO implement
		     */
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		    
//		    FileOutputFormat.setCompressOutput(job, true);
//		    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//		    
//		    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		    
		    if(compress){
		    	FileOutputFormat.setCompressOutput(job, true);
			    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		    }
		    
		    job.setNumReduceTasks(0);
		    
		    boolean success = job.waitForCompletion(true);
		    return success ? 0 : 1;
		  }

		  public static void main(String[] args) throws Exception {
		    int exitCode = ToolRunner.run(new Configuration(), new CreateSequenceFileParameter(), args);
		    System.exit(exitCode);
		  }
}
