package chapter6.example1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class UserIdBinningMapper extends Mapper<Object, Text, Text, Text> {
	public static final String AVERAGE_POSTS_PER_USER = "avg.posts.per.user";

	private double average = 0;
	private MultipleOutputs<Text, Text> mos = null;
	private Text outKey = new Text();
	private Text outValue = new Text();

	public static void setAveragePostsPerUser(Job job, double avg) {
		job.getConfiguration().set(AVERAGE_POSTS_PER_USER, Double.toString(avg));
	}

	public static double getAveragePostsPerUser(Configuration conf) {
		return Double.parseDouble(conf.get(AVERAGE_POSTS_PER_USER));
	}

	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		average = getAveragePostsPerUser(context.getConfiguration());
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String arr[] = value.toString().split("\t");
		long posts = Long.parseLong(arr[1]);
		outKey.set(arr[0]);
		outValue.set(arr[1]);
		if (posts > average) {
			mos.write(JobChainingDriver.MULTIPLE_OUTPUTS_ABOVE_NAME, outKey, outValue);
		} else {
			mos.write(JobChainingDriver.MULTIPLE_OUTPUTS_BELOW_NAME, outKey, outValue);
		}
	}

	@Override
	protected void cleanup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}
}
