package sorting;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import common.JobBuilder;

public class SortByTemperatureUsingHashPartitioner extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		getConf().set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");  
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), arg0);
		if (job == null) {
			return -1;
		}
		
//		 job.setMapperClass(CleanerMapper.class);
		 job.setInputFormatClass(KeyValueTextInputFormat.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
//		 job.setNumReduceTasks(30);

//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setCompressOutput(job, true);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(), args);
		System.exit(exitCode);
	}
}
