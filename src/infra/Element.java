package infra;

import java.io.Serializable;

public class Element implements Comparable<Element>, Serializable{

	private static final long serialVersionUID = 7369752146971856753L;
	private int value;
	
	public void setValue(int value) {
		this.value = value;
	}

	public Element(int value) {
		this.value = value;
	}

	@Override
	public int compareTo(Element e) {
		if (this.value < e.value) {
			return -1;
		}
		if (this.value == e.value){
			return 0;
		}
		return 1;
	}
	
	public int getValue() {
		return value;
	}
	
	@Override 
	public String toString() {
		return String.valueOf(this.value);
	}
}
