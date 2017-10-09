package chapter3.example4;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {

	private TreeMap<Integer, Text> recordMap = new TreeMap<Integer, Text>();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String[] stringArr = value.toString().split("\t");
		String userId = stringArr[1];
		String reputation = stringArr[0];
		if (userId == null || reputation == null) {
			return;
		}
		recordMap.put(Integer.parseInt(reputation), new Text(value));
		if (recordMap.size() > 10) {
			recordMap.remove(recordMap.firstKey());
		}
	}
	
	@Override
	protected void cleanup(Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text text : recordMap.values()) {
			context.write(NullWritable.get(), text);
		}
	}
}
