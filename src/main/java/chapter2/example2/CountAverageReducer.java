package chapter2.example2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountAverageReducer
		extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
	private CountAverageTuple tuple = new CountAverageTuple();

	@Override
	protected void reduce(IntWritable key, Iterable<CountAverageTuple> values,
			Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		long cnt = 0;
		double sum = 0;
		for (CountAverageTuple val : values) {
			sum += val.getAverage() * val.getCount();
			cnt += val.getCount();
		}
		tuple.setId(key.get());
		tuple.setCount(cnt);
		tuple.setAverage(sum / cnt);
		context.write(key, tuple);
	}
}
