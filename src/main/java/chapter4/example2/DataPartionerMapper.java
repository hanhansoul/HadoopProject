package chapter4.example2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataPartionerMapper extends Mapper<Object, Text, IntWritable, Text>{
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arr = value.toString().split("\t");
		IntWritable outkey = new IntWritable(Integer.parseInt(arr[0]));
		context.write(outkey, value);
	}
}
