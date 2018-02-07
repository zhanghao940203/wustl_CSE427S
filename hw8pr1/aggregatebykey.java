package stubs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class aggregatebykey extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new topNdriver(), args);
		System.exit(exitCode);
	}
	public int run(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		Job aggJob = Job.getInstance(conf1, "Top N List");

		aggJob.setJarByClass(getClass());
		FileInputFormat.addInputPath(aggJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(aggJob, new Path(args[1]));

		aggJob.setMapperClass(aggregatebykeymapper.class);
		aggJob.setReducerClass(aggregatebykeyreducer.class);

		aggJob.setOutputKeyClass(Text.class);
		aggJob.setOutputValueClass(DoubleWritable.class);
		Path outputPath= new Path("Firstoutput");

		aggJob.waitForCompletion(true);


		boolean status = aggJob.waitForCompletion(true);
		return status ? 0 : 1;
	}
}
