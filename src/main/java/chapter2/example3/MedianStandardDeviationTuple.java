package chapter2.example3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MedianStandardDeviationTuple implements Writable {

	private double median;
	private double stdDev;

	/**
	 * @return the median
	 */
	public double getMedian() {
		return median;
	}

	/**
	 * @param median
	 *            the median to set
	 */
	public void setMedian(double median) {
		this.median = median;
	}

	/**
	 * @return the stdDev
	 */
	public double getStdDev() {
		return stdDev;
	}

	/**
	 * @param stdDev
	 *            the stdDev to set
	 */
	public void setStdDev(double stdDev) {
		this.stdDev = stdDev;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		median = in.readDouble();
		stdDev = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(median);
		out.writeDouble(stdDev);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return median + "\t" + stdDev;
	}
}
