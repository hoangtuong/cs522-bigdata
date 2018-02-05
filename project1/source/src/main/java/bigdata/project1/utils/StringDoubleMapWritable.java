package bigdata.project1.utils;

import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringDoubleMapWritable extends MapWritable {
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("[");
		for (Map.Entry<Writable, Writable> entry : this.entrySet()) {
			sb.append("(");
			sb.append(((Text)entry.getKey()).toString());
			sb.append(", ");
			sb.append(((DoubleWritable)entry.getValue()).get());
			sb.append(")");
		}
		sb.append("]");
		
		return sb.toString();
	}
}
