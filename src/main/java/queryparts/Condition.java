package queryparts;

import java.io.Serializable;
import java.util.Date;

import defines.RestrOperation;

public class Condition implements Serializable {

	private String attribute;
	private String operator;
	private String val;

	public Boolean evalCondition(Object tval) {
		
		boolean evaluateCondition = false;
		
		switch (operator) {
        	
			case RestrOperation.EQUAL:
				
				if(tval instanceof Integer) {
					Integer val = Integer.valueOf(this.val);	
					evaluateCondition = val.equals(tval);
				}
				else if (tval instanceof Double) {
					
				}
				else if(tval instanceof Float) {
					
				}
				else if(tval instanceof String) {
					evaluateCondition = val.equals(tval);
				}
				else if(tval instanceof Date) {
					
				}
        		break;
        		
			case RestrOperation.NOT_EQUAL:
				
				if(tval instanceof Integer) {
					Integer val = Integer.valueOf(this.val);	 
					evaluateCondition = (val != tval);
				}
				else if (tval instanceof Double) {
					
				}
				else if(tval instanceof Float) {
					
				}
				else if(tval instanceof String) {
					evaluateCondition = (!val.equals(tval));
				}
				else if(tval instanceof Date) {
					
				}
        		break;
        		
			case RestrOperation.LESS_THAN:
				
				if(tval instanceof Integer) {
					Integer val = Integer.valueOf(this.val);	
					evaluateCondition = (val > (Integer) tval);
				}
				else if (tval instanceof Double) {
					
				}
				else if(tval instanceof Float) {
					
				}
				else if(tval instanceof String) {
				}
				else if(tval instanceof Date) {
					
				}
        		break;
        		
			case RestrOperation.GREATER_THAN:
				
				if(tval instanceof Integer) {
					Integer val = Integer.valueOf(this.val);	
					evaluateCondition = (val < (Integer) tval);
				}
				else if (tval instanceof Double) {
					
				}
				else if(tval instanceof Float) {
					
				}
				else if(tval instanceof String) {
					
				}
				else if(tval instanceof Date) {
					
				}
        		break;
        		
			case RestrOperation.LESS_THAN_OR_EQUAL:
				
				if(tval instanceof Integer) {
					Integer val = Integer.valueOf(this.val);	
					evaluateCondition = (val >= (Integer) tval);
				}
				else if (tval instanceof Double) {
					
				}
				else if(tval instanceof Float) {
					
				}
				else if(tval instanceof String) {
					
				}
				else if(tval instanceof Date) {
					
				}
        		break;   
        		
			case RestrOperation.GREATER_THAN_OR_EQUAL:
				
				if(tval instanceof Integer) {
					Integer val = Integer.valueOf(this.val);	
					evaluateCondition = (val <= (Integer) tval);
				}
				else if (tval instanceof Double) {
					
				}
				else if(tval instanceof Float) {
					
				}
				else if(tval instanceof String) {
					
				}
				else if(tval instanceof Date) {
					
				}
        		break;
        		
        	default:
            	break;
		}
            
		return evaluateCondition;
	}

	public void createARestrictionCondition(String arestriction) {

		arestriction = arestriction.replaceAll("\\s", "").replaceAll("'", "").replace("(x)", "");

		if (arestriction.contains(RestrOperation.GREATER_THAN_OR_EQUAL)) {
			String[] splitted = arestriction.split(RestrOperation.GREATER_THAN_OR_EQUAL);
			attribute = splitted[0];
			operator = RestrOperation.GREATER_THAN_OR_EQUAL;
			val = splitted[1];
		} else if (arestriction.contains(RestrOperation.LESS_THAN_OR_EQUAL)) {
			String[] splitted = arestriction.split(RestrOperation.LESS_THAN_OR_EQUAL);
			attribute = splitted[0];
			operator = RestrOperation.LESS_THAN_OR_EQUAL;
			val = splitted[1];
		} else if (arestriction.contains(RestrOperation.GREATER_THAN)) {
			String[] splitted = arestriction.split(RestrOperation.GREATER_THAN);
			attribute = splitted[0];
			operator = RestrOperation.GREATER_THAN;
			val = splitted[1];
		} else if (arestriction.contains(RestrOperation.LESS_THAN)) {
			String[] splitted = arestriction.split(RestrOperation.LESS_THAN);
			attribute = splitted[0];
			operator = RestrOperation.LESS_THAN;
			val = splitted[1];
		} else if (arestriction.contains(RestrOperation.NOT_EQUAL)) {
			String[] splitted = arestriction.split(RestrOperation.NOT_EQUAL);
			attribute = splitted[0];
			operator = RestrOperation.NOT_EQUAL;
			val = splitted[1];
		} else if (arestriction.contains(RestrOperation.EQUAL)) {
			String[] splitted = arestriction.split(RestrOperation.EQUAL);
			attribute = splitted[0];
			operator = RestrOperation.EQUAL;
			val = splitted[1];
		} else {
			System.out.println("ERROR: Not Supporting Operator in Query Attribute Restriction");
		}
	}

	public String getAttribute() {
		return attribute;
	}

	public String getOperator() {
		return operator;
	}

	public String getValue() {
		return val;
	}

	@Override
	public String toString() {
		return "Condition [on attribute: " + attribute + ", operator: '" + operator + "', value: '" + val + "']";
	}
}
