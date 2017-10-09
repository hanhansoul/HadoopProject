package chapter3.example5;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctMapper extends Mapper<Object, Text, Text, NullWritable>{
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(value, NullWritable.get());
	}
}
