package chapter5.example1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoiningDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println(
					"Usage: ReduceSideJoin <dataset A> <dataset B> <out> [inner|leftouter|rightouter|fullouter|anti]");
			System.exit(1);
		}

		String joinType = otherArgs[3];
		if (!(joinType.equalsIgnoreCase("inner") || joinType.equalsIgnoreCase("leftouter")
				|| joinType.equalsIgnoreCase("rightouter") || joinType.equalsIgnoreCase("fullouter")
				|| joinType.equalsIgnoreCase("anti"))) {
			System.err.println("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
			System.exit(2);
		}

		Job job = new Job(conf, "Reduce Side Join");

		// Configure the join type
		job.getConfiguration().set("join.type", joinType);
		job.setJarByClass(ReduceSideJoiningDriver.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, InputAMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, InputBMapper.class);

		job.setReducerClass(ReduceSideJoiningReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
