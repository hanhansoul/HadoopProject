package chapter2.example3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianStandardDeviationMapper
		extends Mapper<Object, Text, IntWritable, DoubleWritable> {

	private IntWritable id = new IntWritable();
	private DoubleWritable v = new DoubleWritable();

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] arr = line.split("\t");

		if (arr.length == 2) {
			id.set(Integer.valueOf(arr[0]));
			v.set(Double.valueOf(arr[1]));
		}
		context.write(id, v);
	}
}
