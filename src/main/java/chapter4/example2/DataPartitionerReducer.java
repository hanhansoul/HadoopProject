package chapter4.example2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataPartitionerReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (Text value : values) {
			context.write(value, NullWritable.get());
		}
	}
}
