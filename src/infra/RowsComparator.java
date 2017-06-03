package infra;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeSet;

public class RowsComparator implements Comparator<CubeGroup>, Serializable { //TODO check if this class is necessary

	private static final long serialVersionUID = -5175560217192302784L;
	private TreeSet<Integer> dimensionToCompare = null;
	
	public void setDimensionToCompare(ArrayList<Integer> newOrder) {	
		dimensionToCompare = new TreeSet<Integer>(newOrder);		
	}
	
	@Override
	public int compare(CubeGroup r1, CubeGroup r2) {
		for (Integer d : dimensionToCompare) {
			int result = r1.getElement(d).compareTo(r2.getElement(d));
			if (0 != result) {
				return result;
			}
		}
		
		return 0; 
	}

}
