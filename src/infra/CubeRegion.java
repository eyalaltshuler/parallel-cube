package infra;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class CubeRegion implements Comparable<CubeRegion>,Serializable {

	private static final long serialVersionUID = -8882908421079951376L;
	private ArrayList<CubeRegion> ancestors;	
	private ArrayList<CubeRegion> children;	
	private HashSet<Integer> region;	
	private TreeMap<CubeGroup,Operator> skews;
	private TreeSet<CubeGroup> buckets;
	
	private boolean isCovered = false;
	private boolean alreadyVisited = false;

	public boolean isAlreadyVisited() {
		return alreadyVisited;
	}

	public void setAlreadyVisited(boolean alreadyVisited) {
		this.alreadyVisited = alreadyVisited;
	}

	public boolean isCovered() {
		return isCovered;
	}

	public void setIsCovered(boolean isCovered) {
		this.isCovered = isCovered;
	}

	public CubeRegion(Set<Integer> dims) {
		region = new HashSet<Integer>(dims);
		ancestors = new ArrayList<CubeRegion>();
		children = new ArrayList<CubeRegion>();
		skews = new TreeMap<CubeGroup,Operator>();
	}
	
	public TreeMap<CubeGroup,Operator> getSkews() {
		return skews;
	}

	public void setSkews(TreeMap<CubeGroup,Operator> skews) {
		this.skews = skews;
	}
	
	public TreeSet<CubeGroup> getBuckets() {
		return buckets;
	}

	public void setBuckets(TreeSet<CubeGroup> buckets) {
		this.buckets = buckets;
	}
	
	public HashSet<Integer> getRegion() {
		return region;
	}
	
	public ArrayList<CubeRegion> getAncestors() {
		return ancestors;
	}

	public ArrayList<CubeRegion> getChildren() {
		return children;
	}
	
	public void addBucket(CubeGroup r) {
		buckets.add(r);
	}
	
	public void addToSkew(CubeGroup r, Operator o) {
		if (skews.get(r) == null) {
			skews.put(r,o);
		}
		Element value = r.getMeasure();
		skews.get(r).update(value);
	}
	
	public void resetSkews() {
		for (Map.Entry<CubeGroup, Operator> pair : skews.entrySet()) {
			pair.setValue(null);
		}
	}
	
	public boolean containsSkew(CubeGroup r) {
		return skews.keySet().contains(r);
	}
	
	public boolean containsBucket(CubeGroup r) {
		return buckets.contains(r);
	}
	
	public int getBucket(CubeGroup r) {
		return buckets.headSet(r).size();
	}
	
	public void addAncestor(CubeRegion ancestor) {
		ancestors.add(ancestor);
	}
	
	public void addChild(CubeRegion child) {
		children.add(child);
	}
	
	public int getNumOfAttributes() {
		return this.region.size();
	}
	
	public boolean isChild(CubeRegion other) {
		return other.getRegion().containsAll(this.getRegion());
	}
	
	public String getSchema() {
		String result = "";
		for (Integer d : this.region) {
			result += d.toString(); 
		}
		result += " - has " + this.getChildren().size() + " children";
		return result;
	}

	@Override
	public int compareTo(CubeRegion o) {
		TreeSet<Integer> myDims = new TreeSet<Integer>(region);
		TreeSet<Integer> hisDims = new TreeSet<Integer>(o.getRegion());
		
		if (myDims.size() != hisDims.size()) {
			return myDims.size() - hisDims.size();
		}
		
		Iterator<Integer> myIt = myDims.iterator();
		Iterator<Integer> hisIt = hisDims.iterator();
		while (myIt.hasNext() && hisIt.hasNext()) {
			Integer my = myIt.next();
			Integer his = hisIt.next();
			if (my == his) {
				continue;
			}
			return my - his;
		}
		if (!myIt.hasNext() && !hisIt.hasNext()) {
			return 0;
		}
		if (!myIt.hasNext()) {
			return -1;
		}
		if (!hisIt.hasNext()) {
			return 1;
		}
		return 0;
		
	}
	
	@Override
	public String toString() {
		ArrayList<String> tmp = new ArrayList<String>();
		for (Integer d : this.getRegion()) {
			tmp.add(d.toString());
		}
		return tmp.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((region == null) ? 0 : region.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CubeRegion other = (CubeRegion) obj;
		if (region == null) {
			if (other.region != null)
				return false;
		} else if (!region.equals(other.region))
			return false;
		return true;
	}

	public CubeRegion getNonSkewedParent(CubeGroup group) {
		TreeSet<CubeRegion> sortedAncestors = new TreeSet<CubeRegion>(getAncestors());
		for (CubeRegion ancestor : sortedAncestors) {
			CubeGroup tmp = group.getSubGroup(ancestor.getRegion());
			if (!ancestor.containsSkew(tmp)) {
				return ancestor;
			}
		}
		return null;
	}

	public boolean directChild(CubeRegion child) {
		TreeSet<Integer> mySortedDims = new TreeSet<Integer>(getRegion());
		TreeSet<Integer> childSortedDims = new TreeSet<Integer>(child.getRegion());
		return (childSortedDims.headSet(mySortedDims.last(),true).size() == mySortedDims.size());
	}

}
