package comp9313.proj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StringPair implements WritableComparable<StringPair>{
	
	private String first;
	private String second;
	
	public StringPair(){
		
	}
	
	public StringPair(String first, String second) {
		this.set(first, second);
	}
	
	public void set(String one, String two) {
		first = one;
		second = two;
	}
	
	public String getFirst() {
		return this.first;
	}
	
	public String getSecond() {
		return this.second;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		String[] strings = WritableUtils.readStringArray(input);
		this.first = strings[0];
		this.second = strings[1];
	}

	@Override
	public void write(DataOutput output) throws IOException {
		String[] strings = new String[] {first, second};
		WritableUtils.writeStringArray(output, strings);
	}
	
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(this.first + " " + this.second);
		return sb.toString();
	}
	
	private int compare(String s1, String s2) {
		if (s1 == null && s2 != null) {
			return -1;
		} else if (s1 != null && s2 == null) {
			return 1;
		} else if (s1 == null && s2 == null) {
			return 0;
		} else {
			return s1.compareTo(s2);
		}
	}

	@Override
	public int compareTo(StringPair string) {
		int cmp = compare(this.getFirst(), string.getFirst());
		if (cmp != 0) {
			return cmp;
		}
		return compare(this.getSecond(), string.getSecond());
	}
//	@Override
//	public int compareTo(StringPair s2) {
//		int cmp = this.getFirst().compareTo(s2.getFirst());
//		if (cmp != 0) {
//			return cmp;
//		}
//		if (this.getSecond().equals("**")) {
//			return -1;
//		} else if (s2.getSecond().equals("**")) {
//			return 1;
//		}
//		return this.getSecond().compareTo(s2.getSecond());
//	}
}
