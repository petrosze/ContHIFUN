package evaluator.mapreduce.conceptualschema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Key implements Serializable{

	private Map<String, String> gAttr;
	
	public Key(String attrName, String attrValue) {
		gAttr = new HashMap<String, String>();
		gAttr.put(attrName, attrValue);
	}
	
	public Key() {
		gAttr = new HashMap<String, String>();
	}

	public void addAttributeValue(String attrName, String attrValue) {
		this.gAttr.put(attrName, attrValue);
	}
	
	public String getAttributeValue(String attrName) {
		if(this.gAttr != null) return gAttr.get(attrName);
		else return null;
	}
	
	public String getValue() {
		if (this.gAttr == null) return null;
		else {
			Map.Entry<String,String> entry = this.gAttr.entrySet().iterator().next();
			 return entry.getValue();
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((gAttr == null) ? 0 : gAttr.hashCode());
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
		Key other = (Key) obj;
		if (gAttr == null) {
			if (other.gAttr != null)
				return false;
		} else if (!gAttr.equals(other.gAttr))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Key [gAttr=" + gAttr + "]";
	}
}
