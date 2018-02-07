package stubs;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author KrishnaMohan
 * The below mapper creates top 5 list and passes the output to the TopNReducer
 * single reducer key NullWritable ensures that all mappers output will be used by single reducer.
 *
 */
public class aggregatebykeymapper extends Mapper<Object, Text, Text,DoubleWritable> {
	protected void map(Object key, Text value,
			Context context)
					throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String title = value.toString().trim();
		String[] tokens = title.split(",");
		if(tokens.length != 3)
		{return;
		}

		String id = tokens[0];
		double rate = Double.parseDouble(tokens[2]);

		context.write(new Text(id),new DoubleWritable(rate));
	}
}
