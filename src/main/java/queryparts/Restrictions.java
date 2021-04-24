package queryparts;

import java.io.Serializable;
 


public class Restrictions implements Serializable{

	private String arestriction;  // Attribute Restriction as string
	private String rrestriction;  // Result Restriction as string
	
	private Condition acondition;   // Attribute Restriction in three parts: <attribute name, operator, value>
	//private Condition rcondition; // ....

	
	public Restrictions() {
	}
	
	public void createRestrictions() {
		
		if(arestriction != null)
			this.acondition.createARestrictionCondition(arestriction);
			
		//this.rcondition.createRRestrictionCondition(rrestriction);
	}
	
	public void addARestriction(String arestriction) {
		this.arestriction = arestriction;
		this.acondition = new Condition();
	}
	
	public void addRRestriction(String rrestriction) {
		this.rrestriction = rrestriction;
		//this.rcondition = new Condition();
	}

	public String getInitialARestriction() {
		if(arestriction == null) return null;
		else return arestriction;
	}

	public String getInitialRRestriction() {
		if(rrestriction == null) return null;
		else return rrestriction;
	}
	
	@Override
	public String toString() {
		return "Restriction [arestriction=" + arestriction + ", rrestriction=" + rrestriction + "]";
	}

	public Condition getACondition() {
		return this.acondition;
	}
	
	public String getARestrictionAsString() {
		if(this.acondition != null) {
			return this.acondition.toString();
		} else 
			return null;
	}
	
	public String getARString() {
		return this.arestriction;
	}
	
	public String getRRestrictionAsString() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public boolean containsARestriction() {
		if(this.acondition == null) return false;
		else return true;
	}


}

