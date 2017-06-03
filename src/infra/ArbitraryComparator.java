package infra;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

public class ArbitraryComparator extends WritableComparator{
	
	public ArbitraryComparator() {
		super(BytesWritable.class);
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)  {
		return -1;
	}
}
