package chapter4.example4new;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SampleMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	private IntWritable outkey = new IntWritable();
	private IntWritable outvalue = new IntWritable();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// String[] valueArr = value.toString().split("\t");
		outkey.set(Integer.parseInt(value.toString()));
		outvalue.set(Integer.parseInt(value.toString()));
		context.write(outkey, outvalue);
	}
}
