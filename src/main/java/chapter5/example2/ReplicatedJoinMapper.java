package chapter5.example2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {

	private HashMap<String, String> userIdInfo = new HashMap<String, String>();
	private Text outValue = new Text();
	private String joinType = null;
	
	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length == 0) {
			throw new RuntimeException("User information is absent in DistributedCache");
		}

		for (Path p : files) {
			String pp = p.toString().split(":")[1];
//			String pp = p.toString();
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(new FileInputStream(new File(pp))));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] arr = line.split("\t");
				if (arr.length == 2)
					userIdInfo.put(arr[0], arr[1]);
			}
		}
		joinType = context.getConfiguration().get("join.type");
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arr = value.toString().split("\t");
		String info = userIdInfo.get(arr[0]);
		if (info != null) {
			outValue.set(info);
			context.write(value, outValue);
		} else if (joinType.equalsIgnoreCase("leftouter")) {
			context.write(value, new Text(""));
		}
	}
}
