package chapter4.example4new;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ValueReducer extends Reducer<Text, Text, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (Text value : values) {
			context.write(value, NullWritable.get());
		}
	}
	
//	@Override
//	protected void reduce(IntWritable key, Iterable<IntWritable> values, 
//			Reducer<IntWritable, IntWritable, IntWritable, NullWritable>.Context context)
//			throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		for (IntWritable value : values) {
//			context.write(value, NullWritable.get());
//		}
//	}
}
