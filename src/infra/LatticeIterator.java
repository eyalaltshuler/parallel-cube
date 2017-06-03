package infra;

import java.util.Iterator;
import java.util.Map;

public class LatticeIterator implements Iterator<CubeRegion>{
	private Iterator<Integer> regionsExternalLevelsIt;
	private Iterator<Map.Entry<CubeRegion, Table>> regionsInternalLevelIt;
	private Lattice l = null;
	
	public LatticeIterator(Lattice l) {
		this.l = l;
		regionsExternalLevelsIt = l.getAllCuboids().keySet().iterator();
		Integer i = regionsExternalLevelsIt.next();
		regionsInternalLevelIt = l.getAllCuboids().get(i).entrySet().iterator();
	}
	
	@Override
	public boolean hasNext() {
		if (!regionsInternalLevelIt.hasNext() && !regionsExternalLevelsIt.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public CubeRegion next() {
		if (regionsInternalLevelIt.hasNext()) {
			return regionsInternalLevelIt.next().getKey();
		}
		if (!regionsExternalLevelsIt.hasNext()) {
			return null;
		}
		Integer i = regionsExternalLevelsIt.next();
		regionsInternalLevelIt = l.getAllCuboids().get(i).entrySet().iterator();
		return regionsInternalLevelIt.next().getKey();
	}

}
