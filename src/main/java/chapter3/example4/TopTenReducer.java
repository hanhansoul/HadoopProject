package chapter3.example4;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	private TreeMap<Integer, Text> recordMap = new TreeMap<Integer, Text>();

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (Text value : values) {
			String[] stringArr = value.toString().split("\t");
			String userId = stringArr[1];
			String reputation = stringArr[0];
			if (userId == null || reputation == null) {
				continue;
			}
			recordMap.put(Integer.parseInt(reputation), new Text(value));
			if (recordMap.size() > 10) {
				recordMap.remove(recordMap.firstKey());
			}
		}
		for (Text text : recordMap.descendingMap().values()) {
			context.write(NullWritable.get(), text);
		}
	}

}
