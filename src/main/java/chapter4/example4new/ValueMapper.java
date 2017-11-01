package chapter4.example4new;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ValueMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
	private IntWritable outkey = new IntWritable();
	private IntWritable outvalue = new IntWritable();

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		outkey.set(Integer.parseInt(key.toString()));
		outvalue.set(Integer.parseInt(value.toString()));
		context.write(outkey, outvalue);
	}

}
