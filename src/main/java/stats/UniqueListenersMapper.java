package stats;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueListenersMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	private enum COUNTER {
		INVALID_RECORD_COUNT
	}
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
				String[] parts = value.toString().split("[|]");
				if (parts.length == 5) {
					IntWritable trackId = new IntWritable(Integer.parseInt(parts[MusicConstants.TRACK_ID]));
					IntWritable listenerId = new IntWritable(Integer.parseInt(parts[MusicConstants.USER_ID]));
					context.write(trackId, listenerId);
				} else {
//					context.getCounter("COUNTERS.INVALID_RECORD_COUNT", null);
				}
	}

}
