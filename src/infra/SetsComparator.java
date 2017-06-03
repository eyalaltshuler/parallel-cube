package infra;

import java.util.Comparator;
import java.util.Set;

public class SetsComparator implements Comparator<Set<?>>{

	@Override
	public int compare(Set<?> o1, Set<?> o2) {
		return ( o1.size() - o2.size() );
	}

}
