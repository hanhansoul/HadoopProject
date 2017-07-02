package stats;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueListenersReducer
		extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	protected void reduce(IntWritable trackId, Iterable<IntWritable> userIds,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		Set<Integer> userIdSet = new HashSet<Integer>();
		for (IntWritable userId : userIds) {
			userIdSet.add(userId.get());
		}
		context.write(trackId, new IntWritable(userIdSet.size()));
	}
}
