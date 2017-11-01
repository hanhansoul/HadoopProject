package chapter5.example2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReplicatedJoinDriver {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 4) {
			System.err.println("Usage: ReplicatedJoin <user data> <comment data> <out> [inner|leftouter]");
			System.exit(1);
		}

		String joinType = otherArgs[3];
		if (!(joinType.equalsIgnoreCase("inner") || joinType.equalsIgnoreCase("leftouter"))) {
			System.err.println("Join type not set to inner or leftouter");
			System.exit(2);
		}
		
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);
//		DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(), conf);
//		DistributedCache.setLocalFiles(conf, otherArgs[0]);
		
		// Configure the join type
		Job job = new Job(conf, "Replicated Join");
		job.getConfiguration().set("join.type", joinType);
		job.setJarByClass(ReplicatedJoinDriver.class);

		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Configure the DistributedCache
		
//		job.addCacheFile(new Path(otherArgs[0]).toUri());

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
