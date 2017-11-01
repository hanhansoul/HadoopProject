package chapter5.example1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InputAMapper extends Mapper<Object, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] valueArr = value.toString().split("\t");
		outKey.set(valueArr[0]);
		outValue.set("A\t" + value.toString());
		context.write(outKey, outValue);
	}
}
