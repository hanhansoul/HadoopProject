package chapter6.example1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserIdCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	public static final String USERS_COUNTER_NAME = "Users";
	private LongWritable outValue = new LongWritable();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		context.getCounter(JobChainingDriver.AVERAGE_CALC_GROUP, USERS_COUNTER_NAME).increment(1);
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		outValue.set(sum);
		context.write(key, outValue);
	}
}
