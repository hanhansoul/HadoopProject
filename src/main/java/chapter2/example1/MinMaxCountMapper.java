package chapter2.example1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends 
	Mapper<Object, Text, Text, MinMaxCountTuple>{
	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, MinMaxCountTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] arr = line.split("\t");
		outUserId.set(arr[0]);
		Date creationDate = null;
		try {
			creationDate = frmt.parse(arr[1]);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		outTuple.setMin(creationDate);
		outTuple.setMax(creationDate);
		outTuple.setCount(1);
		context.write(outUserId, outTuple);
	}
}
