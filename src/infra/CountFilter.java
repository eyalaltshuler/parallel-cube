package infra;

public class CountFilter implements Filter {

	private int threshold;
	
	public CountFilter(int threshold) {
		this.threshold = threshold;
	}
	
	@Override
	public boolean isValid(CubeGroup r) {
		return (r.getMeasure().getValue() >= threshold);
	}

}
