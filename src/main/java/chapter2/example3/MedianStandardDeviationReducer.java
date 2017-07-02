package chapter2.example3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStandardDeviationReducer
		extends Reducer<IntWritable, DoubleWritable, IntWritable, MedianStandardDeviationTuple> {
	private MedianStandardDeviationTuple tuple = new MedianStandardDeviationTuple();
	private ArrayList<Double> list = new ArrayList<Double>();

	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
			Reducer<IntWritable, DoubleWritable, IntWritable, MedianStandardDeviationTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int cnt = 0;
		double sum = 0;
		list.clear();
		for (DoubleWritable val : values) {
			sum += val.get();
			cnt++;
			list.add(val.get());
		}
		Collections.sort(list);
		if (list.size() % 2 == 0) {
			tuple.setMedian((list.get(cnt / 2 - 1) + list.get(cnt / 2)) / 2.0);
		} else {
			tuple.setMedian(list.get(cnt / 2));
		}
		double mean = sum / cnt;
		double sumOfSquares = 0;
		for (Double val : list) {
			sumOfSquares += (val - mean) * (val - mean);
		}
		tuple.setStdDev(Math.sqrt(sumOfSquares / cnt));
		context.write(key, tuple);
	}
}
