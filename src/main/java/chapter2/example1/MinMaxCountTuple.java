package chapter2.example1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.Writable;
import org.apache.jasper.tagplugins.jstl.core.Out;

public class MinMaxCountTuple implements Writable {

	private Date min = new Date();
	private Date max = new Date();
	private long count = 0;

	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");

	/**
	 * @return the min
	 */
	public Date getMin() {
		return min;
	}

	/**
	 * @param min
	 *            the min to set
	 */
	public void setMin(Date min) {
		this.min = min;
	}

	/**
	 * @return the max
	 */
	public Date getMax() {
		return max;
	}

	/**
	 * @param max
	 *            the max to set
	 */
	public void setMax(Date max) {
		this.max = max;
	}

	/**
	 * @return the count
	 */
	public long getCount() {
		return count;
	}

	/**
	 * @param count
	 *            the count to set
	 */
	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	}
}
