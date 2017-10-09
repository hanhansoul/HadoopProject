package chapter3.example3;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import util.MRDPUtils;

public class BloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable> {

	private BloomFilter filter = new BloomFilter();

	@Override
	protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
		if (files != null && files.length == 1) {
			System.out.println("Reading Bloom filter from: " + files[0].getPath());
			DataInputStream stream = new DataInputStream(new FileInputStream(files[0].getPath()));
			filter.readFields(stream);
			stream.close();
		} else {
			throw new IOException("Bloom filter file not set in the DistributedCache");
		}
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		String comment = parsed.get("Text");
		if (comment == null) {
			return;
		}
		StringTokenizer tokenizer = new StringTokenizer(comment);
		while (tokenizer.hasMoreTokens()) {
			String cleanWord = tokenizer.nextToken().replaceAll("'", "").replaceAll("[^a-zA-Z]", " ");
			if (cleanWord.length() > 0 && filter.membershipTest(new Key(cleanWord.getBytes()))) {
				context.write(value, NullWritable.get());
				break;
			}
		}
	}

}
