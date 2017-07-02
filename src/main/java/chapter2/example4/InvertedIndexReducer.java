package chapter2.example4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		for (Text val : values) {
			sb.append(val.toString());
			sb.append(" ");
		}
		result.set(sb.toString());
		context.write(key, result);
	}
}
