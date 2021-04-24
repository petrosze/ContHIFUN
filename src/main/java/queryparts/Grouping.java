
package queryparts;
 
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.List; 

import defines.GOperator;
import defines.GType;

public class Grouping implements Serializable{

	private List<String> gAttr;
	private GType gType;
	
	public Grouping() {	
	}
	
	public Grouping(String gPart) {
		
		gAttr = new ArrayList<>();
		
		/* Case 1: Cartesian grouping function 
		 * Example: Q = (g1^g2^g3, m, op), gPart = "g1^g2^g3"
		 * */
		if(gPart.contains(GOperator.CARTESIAN_PRODUCT)) {
			gType = GType.CARTESIAN_GROUPING;
			String [] splitted = gPart.split("\\"+ GOperator.CARTESIAN_PRODUCT);
			gAttr.addAll(Arrays.asList(splitted));
		}
		
		/* Case 2: Composition grouping function 
		 * Example: Q = (g1*g2, m, op), gPart= "g1*g2"
		 * */
		else if(gPart.contains(GOperator.COMPOSITION)) {
			gType = GType.COMPOSITION_GROUPING;
			String [] splitted = gPart.split(GOperator.COMPOSITION);
			gAttr.addAll(Arrays.asList(splitted));
		}
		
		/* Case 3: Composition grouping function 
		 * Example: Q = (proj_A, m, op)
		 * */
		else if(gPart.contains(GOperator.PROJECTION)) {
			gType = GType.PROJECTION_GROUPING;
			String [] splitted = gPart.split("_");
			gAttr.add(splitted[1]);
		}
		
		/* Case 4: Single grouping function
		 * Example: Q = (g, m, op), gPart = "g"
		 * */
		else {
			gType = GType.SIMPLE_GROUPING;
			gAttr.add(gPart);
		}
		
	}

	public Grouping(List<Grouping> allGroupings) {
		this.gAttr = new ArrayList<String>();
		for(Grouping g : allGroupings) {
			gAttr.add(g.gAttr.get(0));
		}
		this.gType = GType.CARTESIAN_GROUPING;
	}


	public Grouping(Grouping g) {
		this.gAttr = g.getGroupingAttributes();
		this.gType = g.gType;
	}

	public List<String> getGroupingAttributes() {
		return gAttr;
	}
	
	public String getGroupingAttribute() {
		return this.gAttr.get(0);
	}
	
	public String getFirstGroupingAttribute() {
		return this.gAttr.get(0);
	}
	
	public String getSecondGroupingAttribute() {
		return this.gAttr.get(1);
	}

	public GType getGroupingType() {
		return this.gType;
	}

	public List<String> getUsefulAttributes() {
		
		List<String> attr = new ArrayList<>(); 
		if(gType == GType.SIMPLE_GROUPING || gType == GType.PROJECTION_GROUPING || gType == GType.CARTESIAN_GROUPING) {
			attr = this.gAttr;
		}
		else if(gType == GType.COMPOSITION_GROUPING) {
			String cAttr = "";
			String prefix ="";
			for(String gName : this.gAttr) {
				cAttr += prefix;
				prefix = "*";
				cAttr += gName;
			}
			attr.add(cAttr);
		}
		return attr;
	}
	
	public String getCompGroupingAsString() {
		String cAttr = "";
		String prefix ="";
		for(String gName : this.gAttr) {
			cAttr += prefix;
			prefix = "*";
			cAttr += gName;
		}
		return cAttr;
	}
	
	public void setGrouping(String groupingAttribute) {
		this.gAttr = new ArrayList<>();
		this.gAttr.add(groupingAttribute);
	}

	public void setGroupingType(GType type) {
		this.gType = type;
	}

	@Override
	public String toString() {
		return "Grouping [gAttr=" + gAttr + ", gType=" + gType + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((gAttr == null) ? 0 : gAttr.hashCode());
		result = prime * result + ((gType == null) ? 0 : gType.hashCode());
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
		Grouping other = (Grouping) obj;
		if (gAttr == null) {
			if (other.gAttr != null)
				return false;
		} else if (!gAttr.equals(other.gAttr))
			return false;
		if (gType != other.gType)
			return false;
		return true;
	}
}
