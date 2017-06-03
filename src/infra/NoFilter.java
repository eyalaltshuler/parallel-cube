package infra;

public class NoFilter implements Filter {

	@Override
	public boolean isValid(CubeGroup r) {
		return true;
	}

}
