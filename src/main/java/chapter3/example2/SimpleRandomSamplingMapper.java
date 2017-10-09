package chapter3.example2;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleRandomSamplingMapper extends Mapper<Object, Text, NullWritable, Text> {

	private double percentage;
	private Random random = new Random();

	@Override
	protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String StringPercentage = context.getConfiguration().get("percentage");
		percentage = Double.parseDouble(StringPercentage);
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (random.nextDouble() <= percentage) {
			context.write(NullWritable.get(), value);
		}
	}

}
