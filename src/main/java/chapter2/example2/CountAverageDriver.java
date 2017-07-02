package chapter2.example2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CountAverageDriver {
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountAverageDriver <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Count Average");
		job.setJarByClass(CountAverageDriver.class);
		job.setMapperClass(CountAverageMapper.class);
		job.setCombinerClass(CountAverageReducer.class);
		job.setReducerClass(CountAverageReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CountAverageTuple.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
