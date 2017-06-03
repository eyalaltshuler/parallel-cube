package infra;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.StringUtils;

public class CubeGroup implements Serializable , Comparable<CubeGroup>  {
	private static final long serialVersionUID = -2086837385556600776L;
	private int id;
	private byte group = 0x0;
	public static TreeSet<Integer> dimensions = new TreeSet<Integer>();

	private TreeMap<Integer,Element> values;
	public TreeMap<Integer, Element> getValues() {
		return values;
	}

	public void setValues(TreeMap<Integer, Element> values) {
		this.values = values;
	}

	private boolean isSkewed = false;
	private Element measure = new Element(0);
	
	public CubeGroup(int id, TreeMap<Integer,Element> values) {
		this.id = id;
		this.values = new TreeMap<Integer,Element>(values);
	}
	
	public CubeGroup(TreeMap<Integer,Element> values) {
		this.values = values;
	}
	
	public CubeGroup(TreeMap<Integer,Element> values, Element measure) {
		this.values = values;
		this.measure = measure;
	}
	
	public CubeGroup(TreeMap<Integer,Element> values, Element measure, boolean isSkewed) {
		this.values = values;
		this.measure = measure;
		this.isSkewed = isSkewed;
		this.group = 0;
	}
	
	public CubeGroup(TreeMap<Integer,Element> values, Element measure, boolean isSkewed, byte group) {
		this.values = values;
		this.measure = measure;
		this.isSkewed = isSkewed;
		this.group = group;
	}
	
	public CubeGroup(int id, TreeMap<Integer,Element> values, Element measure) {
		this.id = id;
		this.values = new TreeMap<Integer,Element>(values);
		this.measure = measure;
		this.isSkewed = false;
		this.group = 0;
	}
	
	public CubeGroup() {
		this.id = 0;
	}
	
	public CubeGroup(CubeGroup other) {
		this.id = other.id;
		this.values = new TreeMap<Integer,Element>(other.values);
		this.group = other.group;
		this.isSkewed = other.isSkewed;
		this.measure = new Element(other.measure.getValue());
	}
			
	public Set<Integer> getDims() {
		TreeSet<Integer> result = new TreeSet<Integer>();
		for (int i=0; i<8; i++) {
			if ( (group & (1 << i)) != 0 ) {
				result.add(i);
			}
		}
		return result;
	}
	
	public Element getMeasure() {
		return measure;
	}

	public void setMeasure(Element measure) {
		this.measure = measure;
	}

	public boolean isSkewed() {
		return isSkewed;
	}

	public void setSkewed(boolean isSkewed) {
		this.isSkewed = isSkewed;
	}	
		
	public int getId() {
		return this.id;
	}
	
	public Element getElement(int index) {
		return this.values.get(index);
	}
	
	public byte getGroup() {
		return group;
	}

	public void setGroup(Set<Integer> columns) {
		this.group = 0;
		for (Integer i : columns) {
			this.group |= (1 << i);
		}
	}
	
	@Override
	public String toString() {
		String result = "";
		String[] attrs = new String[dimensions.size()];
    	for (int i=0; i<attrs.length; i++) {
    		if (getDims().contains(i)) {
    			attrs[i] = String.valueOf(values.get(i).getValue());
    		}
    		else {
    			attrs[i] = "";
    		}
    	}
    	result += StringUtils.join("\t", attrs);
    	result += "\t" + this.measure.getValue() + "\n";
    	return result;
	}
	
	public String toStringWithoutId() {
		String result = "";
		String seperatedAttrs = StringUtils.join("\t", this.values.values());
    	result += seperatedAttrs + "\r\n";
    	return result;
	}
	
	public int firstDifferentAttr(CubeGroup other) {
		return -1;
	}
	
	public CubeGroup getSubGroup(Set<Integer> columns) {
		TreeMap<Integer,Element> attrs = new TreeMap<Integer,Element>();
		for (Integer i : values.keySet()) {
			attrs.put(i, new Element(values.get(i).getValue()));
		}
		CubeGroup result = new CubeGroup(attrs,measure,isSkewed);
		result.setGroup(columns);
		return result;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof CubeGroup)) {
			return false;
		}
		CubeGroup other = (CubeGroup) o;
		if (!(this.group == other.group) ){
			return false;
		}
		for (Integer i : this.getDims()) {
			int myValue = this.values.get(i).getValue();
			int hisValue = other.values.get(i).getValue();
			if (myValue != hisValue) {
				return false;
			}
		}
		return true;
	}
	
	

	private static byte[] intToByteArray(int value) {
	    byte[] result = new byte[4];
	    result[0] = ((byte) (value >> 24));
	    result[1] = ((byte) (value >> 16));
	    result[2] = ((byte) (value >> 8));
	    result[3] = ((byte) value);
		return result;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}

	private static int fromByteArray(byte[] bytes) {
	     return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
	}
	
	public static CubeGroup decode(byte[] data, boolean withMeasure, boolean withId) {
		CubeGroup result = new CubeGroup();
		byte skew = data[0];
		byte group = data[1]; 
		if (skew == 0) {
			result.isSkewed = false;
		}
		else {
			result.isSkewed = true;
		}
		result.group = group;
		int index = 2;
		TreeMap<Integer,Element> values = new TreeMap<Integer,Element>();
		result.values = values;
		if (result.isSkewed) {
			for (Integer d : result.getDims()) {
				byte[] tmp = new byte[4];
				System.arraycopy(data, index, tmp, 0, 4);
				int value = fromByteArray(tmp);
				Element element = new Element(value);
				values.put(d, element);
				index+=4;
			}
		}
		else {
			for (Integer d : dimensions) {
				byte[] tmp = new byte[4];
				System.arraycopy(data, index, tmp, 0, 4);
				int value = fromByteArray(tmp);
				Element element = new Element(value);
				values.put(d, element);
				index+=4;
			}
		}
		
		if (!withMeasure) {
			return result;
		}
		byte[] tmp = new byte[4];
		System.arraycopy(data, index, tmp, 0, 4);
		int value = fromByteArray(tmp);
		Element measure = new Element(value);
		result.setMeasure(measure);

		if (!withId) {
			return result;
		}
		
		tmp = new byte[4];
		System.arraycopy(data, index, tmp, 0, 4);
		int id = fromByteArray(tmp);
		result.id = id;
		
		return result;		
	}
	
	public static byte[] encode(CubeGroup r, boolean withMeasure, boolean withId) {
		int size = 2 + 4 * r.values.keySet().size();
		if (withMeasure) {
			size+=4;
		}
		if (withId) {
			size+=4;
		}
		
		byte[] result = new byte[size];
		int i=0;
		if (r.isSkewed()) {
			result[i++] = 1;
		}
		else {
			result[i++] = 0;
		}
		result[i++] = r.getGroup();
		for (Element value : r.values.values()) {
			byte[] tmp = intToByteArray(value.getValue());
			System.arraycopy(tmp, 0, result, i, 4);
			i+=4;
		}
		
		if (!withMeasure) {
			return result;
		}
		
		byte[] tmp = intToByteArray(r.measure.getValue());
		System.arraycopy(tmp, 0, result, i, 4);
		
		if (!withId) {
			return result;
		}
		
		i+=4;
		tmp = intToByteArray(r.id);
		System.arraycopy(tmp, 0, result, i, 4);
		
		return result;
	}

	public void parseFromString(CubeGroup r, String line) {
		
	}

	public int compareTo(CubeGroup other) {
		if (!(this.group == other.group)) {
			int mySize = this.getDims().size();
			int hisSize = other.getDims().size();
			if (mySize != hisSize) {
				return mySize - hisSize;
			}
			TreeSet<Integer> myDims = new TreeSet<Integer>(getDims());
			TreeSet<Integer> hisDims = new TreeSet<Integer>(other.getDims());
			Iterator<Integer> myDimsIterator = myDims.iterator();
			Iterator<Integer> hisDimsIterator = hisDims.iterator();
			while (myDimsIterator.hasNext()) {
				int myValue = myDimsIterator.next();
				int hisValue = hisDimsIterator.next();
				if (myValue != hisValue) {
					return myValue - hisValue;
				}
			}
		}
		for (Integer index : this.getDims()) {
			Element my = this.values.get(index);
			Element his = other.values.get(index);
			int result = my.compareTo(his);
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}

	
	public static byte[] encodeKey(CubeGroup group) {
		if (group.isSkewed) {
			return encode(group, false, false);
		}
		int size = 2 + 4 * group.getDims().size();
		byte[] result = new byte[size];
		int i=0;
		if (group.isSkewed()) {
			result[i++] = 1;
		}
		else {
			result[i++] = 0;
		}
		result[i++] = group.getGroup();
		for (Integer value : group.getDims()) {
			byte[] tmp = intToByteArray(group.values.get(value).getValue());
			System.arraycopy(tmp, 0, result, i, 4);
			i+=4;
		}
		return result;
	}
	
	public static byte[] encodeValue(CubeGroup group) {
		if (group.isSkewed || (group.group == 0)) {
			return intToByteArray(group.measure.getValue());
		}
		int size = 1;
		byte mask = 0;
		for (Integer i : group.values.keySet()) {
			if (!(group.getDims().contains(i))) {
				size += 4;
				mask |= (byte) (1 << i);
			}
		}
		size += 4; // for measure
		byte[] result = new byte[size];
		result[0] = mask;
		int index = 1;
		for (Integer i : group.values.keySet()) {
			if (!(group.getDims().contains(i))) {
				byte[] tmp = intToByteArray(group.values.get(i).getValue());
				System.arraycopy(tmp, 0, result, index, 4);
				index += 4;
			}
		}
		byte[] tmp = intToByteArray(group.measure.getValue());
		System.arraycopy(tmp, 0, result, index, 4);
		return result;
	}
	
	public static CubeGroup decodeKeyValue(BytesWritable key, BytesWritable value) {
		CubeGroup result = new CubeGroup();
		byte[] keyData = new byte[key.getLength()]; 
		System.arraycopy(key.getBytes(), 0, keyData, 0, key.getLength());
		byte[] valueData = new byte[value.getLength()];
		System.arraycopy(value.getBytes(), 0, valueData, 0, value.getLength());
		byte skew = keyData[0];
		byte group = keyData[1]; 
		if (skew == 0) {
			result.isSkewed = false;
		}
		else {
			result.isSkewed = true;
		}
		result.group = group;
		int index = 2;
		TreeMap<Integer,Element> values = new TreeMap<Integer,Element>();
		result.values = values;
		byte[] tmp = null;
		int valueAsInt = 0;
		for (Integer d : result.getDims()) {
			tmp = new byte[4];
			System.arraycopy(keyData, index, tmp, 0, 4);
			valueAsInt = fromByteArray(tmp);
			Element element = new Element(valueAsInt);
			values.put(d, element);
			index+=4;
		}
		if (result.isSkewed || (result.group == 0)) {
			tmp = new byte[4];
			System.arraycopy(valueData, 0, tmp, 0, 4);
			valueAsInt = fromByteArray(tmp);
			Element measure = new Element(valueAsInt);
			result.setMeasure(measure);
		}
		else {
			byte mask = valueData[0];
			TreeSet<Integer> otherDims = new TreeSet<Integer>();
			for (int i=0; i<8; i++) {
				if (0 != (mask & (byte)(1 << i))) {
					otherDims.add(i);
				}
			}
			Iterator<Integer> otherDimsIt = otherDims.iterator();
			index = 1;
			while (index < valueData.length - 4) {
				tmp = new byte[4];
				System.arraycopy(valueData, index, tmp, 0, 4);
				valueAsInt = fromByteArray(tmp);
				Element element = new Element(valueAsInt);
				values.put(otherDimsIt.next(), element);
				index+=4;
			}
			tmp = new byte[4];
			System.arraycopy(valueData, index, tmp, 0, 4);
			valueAsInt = fromByteArray(tmp);
			Element measure = new Element(valueAsInt);
			result.setMeasure(measure);
		}
		return result;
	}
}
