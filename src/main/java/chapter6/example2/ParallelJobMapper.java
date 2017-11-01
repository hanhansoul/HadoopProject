package chapter6.example2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ParallelJobMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	private DoubleWritable outValue = new DoubleWritable();
	private Text outKey = new Text();
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arr = value.toString().split("\t");
		double val = Double.parseDouble(arr[1]);
		outValue.set(val);
		outKey.set(new Text("avarage:"));
		context.write(outKey, outValue);
	}
}
