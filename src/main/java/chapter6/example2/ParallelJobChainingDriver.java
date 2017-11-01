package chapter6.example2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ParallelJobChainingDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Path inputA = new Path(otherArgs[0]);
		Path inputB = new Path(otherArgs[1]);
		Path outputA = new Path(otherArgs[2]);
		Path outputB = new Path(otherArgs[3]);

		Job jobA = jobSubmit(conf, inputA, outputA);
		Job jobB = jobSubmit(conf, inputB, outputB);

		if (!jobA.isComplete() || !jobB.isComplete()) {
			Thread.sleep(5000);
		}

		if (jobA.isSuccessful()) {
			System.out.println("jobA successed");
		} else {
			System.out.println("jobA failed");
		}

		if (jobB.isSuccessful()) {
			System.out.println("jobB successed");
		} else {
			System.out.println("jobB failed");
		}

		System.out.println(jobA.isSuccessful() && jobB.isSuccessful() ? 0 : 1);
	}

	private static Job jobSubmit(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "Parallel job");
		job.setJarByClass(ParallelJobChainingDriver.class);
		job.setMapperClass(ParallelJobMapper.class);
		job.setReducerClass(ParallelJobReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, input);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, output);

		job.submit();

		return job;
	}
}
