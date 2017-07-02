package chapter2.example2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountAverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {

	private IntWritable outId = new IntWritable();
	private CountAverageTuple tuple = new CountAverageTuple();

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, IntWritable, CountAverageTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] arr = line.split("\t");
		if (arr.length == 3) {
			int id = Integer.valueOf(arr[0]);
			long count = Long.valueOf(arr[1]);
			long val = Long.valueOf(arr[2]);
			tuple.setId(id);
			tuple.setCount(count);
			tuple.setAverage(1.0 * val / count);
			outId.set(id);
		}
		context.write(outId, tuple);
	}
}
