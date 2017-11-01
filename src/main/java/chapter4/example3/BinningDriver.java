package chapter4.example3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BinningDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Binning <posts> <outdir>");
			System.exit(1);
		}

		Job job = new Job(conf, "Binning");
		job.setJarByClass(BinningDriver.class);
		job.setMapperClass(BinningMapper.class);
		job.setNumReduceTasks(0);
		
//		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
				Text.class, NullWritable.class);
//		MultipleOutputs.addNamedOutput(job, "bins", FileOutputFormat.class,
//				Text.class, NullWritable.class);
		
		MultipleOutputs.setCountersEnabled(job, true);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
