package infra;

import java.util.Iterator;

public class CubeGroupIterator implements Iterator<CubeGroup> {

	private Iterator<Table> groupsIt = null;
	private Table currentTable = null;
	private int index = 0;
	
	public CubeGroupIterator(Cube cube) {
		groupsIt = cube.getCuboids().values().iterator();
	}

	@Override
	public boolean hasNext() {
		if (groupsIt.hasNext()) {
			return true;
		}
		else {
			if (currentTable != null) {
				return index < (currentTable.getRows().size()-1);
			}
		}
		return false;
	}

	@Override
	public CubeGroup next() {
		if (currentTable == null) {
			currentTable = groupsIt.next();
			index = 0;
			return currentTable.getRow(index++);
		}
		else {
			if (index == currentTable.getRows().size()) {
				currentTable = groupsIt.next();
				index = 0;
			}
			return currentTable.getRow(index++);
		}
	}

}
