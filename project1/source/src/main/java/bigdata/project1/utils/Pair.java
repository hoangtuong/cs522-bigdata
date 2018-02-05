package bigdata.project1.utils;

public class Pair<U, V> {
	private U left;
	private V right;
	
	public Pair(U left, V right) {
		this.left = left;
		this.right = right;
	}
	
	public U getLeft() {
		return left;
	}
	
	public V getRight() {
		return right;
	}
}
