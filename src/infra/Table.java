package infra;

import java.io.Serializable;
import java.util.ArrayList;

public class Table implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4476093043715609530L;
	private final ArrayList<String> dimensionNames;
	private ArrayList<CubeGroup> data;
	
	public Table(int rowsNum, int dimNum) {
		data = new ArrayList<CubeGroup>(rowsNum);
		dimensionNames = new ArrayList<String>(dimNum);
	}
	
	public Table() {
		data = new ArrayList<CubeGroup>();
		dimensionNames = new ArrayList<String>();
	}
	
	public int getCurrentSize() {
		return this.data.size();
	}
	
	public boolean addRow(CubeGroup r) {
		data.add(r);
		return true;
	}
	
	public String getDimensionName(int dimNum) {
		return this.dimensionNames.get(dimNum);
	}
	
	public void setDimensionName(int dimNum, String name) {
		this.dimensionNames.set(dimNum,name);
	}
	
	public CubeGroup getRow(int index) {
		if ( (index < 0) || (index >= getCurrentSize()) ) {
			return null;
		}
		return this.data.get(index);
	}
	
	public ArrayList<CubeGroup> getRows() {
		return this.data;
	}
	
	@Override
	public String toString() {
		String endline = "\r\n";
		String result = "Table is : " + endline;
		result += "=============================" + endline;
		for (CubeGroup r : this.data) {
			result += r.toString() + endline;
		}
		
		return result;
	}
}
