package chapter3.example1;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrepMapper extends Mapper<Object, Text, NullWritable, Text>{
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String text = value.toString();
		String mapRegex = context.getConfiguration().get("mapregex");
		if(text.matches(mapRegex)) {
			context.write(NullWritable.get(), value);
		}
	}
}
