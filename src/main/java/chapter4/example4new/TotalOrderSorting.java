package chapter4.example4new;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class TotalOrderSorting {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: TotalOrderSorting <user data> <out> <sample rate>");
			System.exit(1);
		}

		Path inputPath = new Path(args[0]);
		Path partitionFile = new Path(args[1] + "_partitions.lst");
		Path outputStage = new Path(args[1] + "_staging");
		Path outputOrder = new Path(args[1]);

		double sampleRate = Double.parseDouble(otherArgs[2]);

		FileSystem.get(new Configuration()).delete(outputOrder, true);
		FileSystem.get(new Configuration()).delete(outputStage, true);
		FileSystem.get(new Configuration()).delete(partitionFile, true);
		
		Job sampleJob = new Job(conf, "TotalOrderSortingStage");
		sampleJob.setJarByClass(TotalOrderSorting.class);
		sampleJob.setMapperClass(SampleMapper.class);
		sampleJob.setNumReduceTasks(0);

		sampleJob.setOutputKeyClass(IntWritable.class);
		sampleJob.setOutputValueClass(IntWritable.class);

		sampleJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(sampleJob, inputPath);
		
		sampleJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(sampleJob, outputStage);
		
//		sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);

		int code = sampleJob.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			Job orderJob = new Job(conf, "TotalOrderSortingStage");
			orderJob.setJarByClass(TotalOrderSorting.class);
			orderJob.setMapperClass(Mapper.class);
//			orderJob.setNumReduceTasks(0);
			orderJob.setReducerClass(ValueReducer.class);
			orderJob.setNumReduceTasks(10);

			orderJob.setPartitionerClass(TotalOrderPartitioner.class);
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);

			orderJob.setOutputKeyClass(Text.class);
			orderJob.setOutputValueClass(Text.class);

			orderJob.setInputFormatClass(KeyValueTextInputFormat.class);
			KeyValueTextInputFormat.setInputPaths(orderJob, outputStage);

			TextOutputFormat.setOutputPath(orderJob, outputOrder);
			orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");

			InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(sampleRate, 10000));

			code = orderJob.waitForCompletion(true) ? 0 : 2;
		}

		System.exit(code);
	}
}
