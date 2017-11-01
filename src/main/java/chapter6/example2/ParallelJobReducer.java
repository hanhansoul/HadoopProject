package chapter6.example2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParallelJobReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		double sum = 0;
		int cnt = 0;
		for (DoubleWritable val : values) {
			sum += val.get();
			cnt++;
		}
		context.write(key, new DoubleWritable(sum / cnt));
	}
}
