package infra;

import java.util.Comparator;

public class RegionComparator implements Comparator<CubeRegion> {

	@Override
	public int compare(CubeRegion o1, CubeRegion o2) {
		return o1.compareTo(o2);
	}

}
