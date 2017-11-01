package chapter4.example4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ValueReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, 
			Reducer<IntWritable, IntWritable, IntWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (IntWritable value : values) {
			context.write(value, NullWritable.get());
		}
	}
}
