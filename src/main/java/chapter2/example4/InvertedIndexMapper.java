package chapter2.example4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
	// private Text outKey = null;
	private Text outValue = new Text();

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arr = value.toString().split("\t");
		if (arr.length < 2) {
			return;
		}
		outValue.set(arr[0]);
		for (int i = 1; i < arr.length; i++) {
			// outKey.set(arr[i]);
			context.write(new Text(arr[i]), outValue);
		}
	}
}
