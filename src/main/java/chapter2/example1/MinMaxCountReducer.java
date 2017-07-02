package chapter2.example1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
	private MinMaxCountTuple result = new MinMaxCountTuple();

	@Override
	protected void reduce(Text key, Iterable<MinMaxCountTuple> values,
			Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		result.setCount(0);
		result.setMax(null);
		result.setMin(null);
		long sum = 0;
		for (MinMaxCountTuple val : values) {
			if (result.getMin() == null || result.getMin().compareTo(val.getMin()) < 0) {
				result.setMin(val.getMin());
			}
			if (result.getMax() == null || result.getMax().compareTo(val.getMax()) > 0) {
				result.setMax(val.getMax());
			}
			sum += val.getCount();
		}
		result.setCount(sum);
		context.write(key, result);
	}
}
