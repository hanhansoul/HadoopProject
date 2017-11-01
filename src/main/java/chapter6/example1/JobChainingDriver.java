package chapter6.example1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class JobChainingDriver {
	public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
	public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "aboveavg";
	public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "belowavg";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: JobChainingDriver <posts> <users> <out>");
			System.exit(2);
		}

		Path postInput = new Path(otherArgs[0]);
		Path userInput = new Path(otherArgs[1]);
		Path outputDirIntermediate = new Path(otherArgs[2] + "_int");
		Path outputDir = new Path(otherArgs[2]);

		Job countingJob = new Job(conf, "JobChaining-Counting");
		countingJob.setJarByClass(JobChainingDriver.class);

		countingJob.setMapperClass(UserIdCountMapper.class);
		countingJob.setCombinerClass(LongSumReducer.class);
		countingJob.setReducerClass(UserIdCountReducer.class);

		countingJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(countingJob, postInput);

		countingJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);
		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(LongWritable.class);

		int code = countingJob.waitForCompletion(true) ? 1 : 0;
		
		if (code == 1) {
			double numRecords = (double) countingJob.getCounters()
					.findCounter(AVERAGE_CALC_GROUP, UserIdCountMapper.RECORDS_COUNTER_NAME).getValue();
			double numUsers = (double) countingJob.getCounters()
					.findCounter(AVERAGE_CALC_GROUP, UserIdCountReducer.USERS_COUNTER_NAME).getValue();
			double averagePostsPerUser = numRecords / numUsers;
			Job binningJob = new Job(new Configuration(), "JobChaining-Binning");
			UserIdBinningMapper.setAveragePostsPerUser(binningJob, averagePostsPerUser);
			
			binningJob.setJarByClass(JobChainingDriver.class);
			binningJob.setMapperClass(UserIdBinningMapper.class);
			binningJob.setNumReduceTasks(0);

			binningJob.setInputFormatClass(TextInputFormat.class);
			TextInputFormat.addInputPath(binningJob, outputDirIntermediate);

			MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_ABOVE_NAME, TextOutputFormat.class, Text.class,
					Text.class);
			MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_BELOW_NAME, TextOutputFormat.class, Text.class,
					Text.class);
			MultipleOutputs.setCountersEnabled(binningJob, true);

			TextOutputFormat.setOutputPath(binningJob, outputDir);

			// Add the user files to the DistributedCache
			// FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
			// for (FileStatus status : userFiles) {
			// DistributedCache.addCacheFile(status.getPath().toUri(),
			// binningJob.getConfiguration());
			// }

			code = binningJob.waitForCompletion(true) ? 1 : 0;
		}

//		FileSystem.get(conf).delete(outputDirIntermediate, true);

	}
}
