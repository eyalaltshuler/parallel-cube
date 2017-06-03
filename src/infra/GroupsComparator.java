package infra;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

public class GroupsComparator extends WritableComparator {
	
	public GroupsComparator() {
		super(BytesWritable.class);
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)  {
		byte firstGroup = b1[s1];
		byte secondGroup = b2[s2];
		boolean firstSkewed = (b1[s1+1] == 0) ? false : true;
		boolean secondSkewed = (b2[s2+1] == 0) ? false : true;
		if (firstSkewed && secondSkewed) {
			return 0;
		}
		if (firstSkewed) {
			return -1;
		}
		if (secondSkewed) {
			return 1;
		}
		
		if (firstGroup != secondGroup) {
			return compareGroups(firstGroup, secondGroup);
		}
		
		int index = 2;
		int dim = 0;
		while (index < l1) {
			int firstValue = readInt(b1, index);
			int secondValue = readInt(b2,index);
			if ((firstGroup & ((byte) 1<<dim)) != 0) {
				if (firstValue != secondValue) {
					return firstValue - secondValue;
				}
			}
			dim++;
			index += 4;
		}
		return 0;
	}
	
	int compareGroups(byte first, byte second) {
		TreeSet<Integer> firstDims = new TreeSet<Integer>();
		TreeSet<Integer> secondDims = new TreeSet<Integer>();
		for (int i=0; i<8; i++) {
			if (0 != (first | (1 << i))) {
				firstDims.add(i);
			}
			if (0 != (second | (1 << i))) {
				secondDims.add(i);
			}
		}
		int firstSize = firstDims.size();
		int secondSize = secondDims.size();
		if (firstSize != secondSize) {
			return firstSize - secondSize;
		}
		Iterator<Integer> firstIt = firstDims.iterator();
		Iterator<Integer> secondIt = secondDims.iterator();
		while (firstIt.hasNext()) {
			int firstValue = firstIt.next();
			int secondValue = secondIt.next();
			if (firstValue != secondValue) {
				return firstValue - secondValue;
			}
		}
		return 0;
	}
}
