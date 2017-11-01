package chapter6.example1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserIdCountMapper extends Mapper<Object, Text, Text, LongWritable> {
	public static final String RECORDS_COUNTER_NAME = "Records";
	private Text outKey = new Text();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arr = value.toString().split("\t");
		outKey.set(arr[0]);
		context.write(outKey, new LongWritable(1));
		context.getCounter(JobChainingDriver.AVERAGE_CALC_GROUP, RECORDS_COUNTER_NAME).increment(1);
	}
}
