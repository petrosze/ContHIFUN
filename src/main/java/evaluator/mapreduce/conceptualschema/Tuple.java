package evaluator.mapreduce.conceptualschema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Tuple implements Serializable{
	
	private Map<String, Object> attributes;
	
	public Tuple() {
		this.attributes = new HashMap<String, Object>();
	}

	public Map<String, Object> getAttributes() {
		return attributes;
	}

	public Object getAttributeValue(String attr) {
		return attributes.get(attr);
	}
	
	public void addAttribute(String name, Object value) {
		this.attributes.put(name, value);
	}

	@Override
	public String toString() {
		return "Tuple [attributes=" + attributes + "]";
	}
}
