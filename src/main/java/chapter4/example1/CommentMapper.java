package chapter4.example1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentMapper extends Mapper<Object, Text, Text, Text> {
	private Text outkey = new Text();
	private Text outValue = new Text();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] valueArr = value.toString().split("\t");
		outkey.set(valueArr[1]);
		outValue.set(valueArr[0] + "\t" + valueArr[2]);
		context.write(outkey, outValue);
	}
}
