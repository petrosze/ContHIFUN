package query;
 
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry; 
import java.util.regex.Pattern;
import java.util.stream.Collectors;
 
import queryparts.Grouping;
import queryparts.Measuring;
import queryparts.Operation;
import queryparts.Restrictions;
import scala.Tuple3;

public class QSet implements Serializable{

	private HashMap<Integer, Query> qset;
	private int qId;
	
	private Restrictions restrictions;
	
	public QSet() {
		this.qset = new HashMap<>();
		this.qId = 1;
	}
	
	public void addCQuery(String squery) { 
		if(isValidSyntax(squery) == false) {
			System.out.println("SYNTAX ERROR: Query " + squery + " is not valid");
			return;
		}
		this.qset.put(this.qId, new Query(squery));
		this.qId++;
	}
	

	public void setARestriction(String arestriction) {
		if(isARestrictionValid(arestriction) == false) {
			System.out.println("SYNTAX ERROR: Attribute Restriction: " + arestriction + " is not valid");
			return;
		}
		
		if(restrictions == null) restrictions = new Restrictions();
		
		restrictions.addARestriction(arestriction);
	}
	
	/**
	 * Create ordered triple Q=(g,m,op) for each query of the QSet
	 * 
	 **/
	public void createTriples() {
		
		if(this.restrictions != null) restrictions.createRestrictions();
		
		for (Entry<Integer, Query> cquery : qset.entrySet()) {
			qset.get(cquery.getKey()).createTriple();
		}
	}
	
	public String getInitialQSet() {
		String qsetAsString = "Initial QSet:\n";
		qsetAsString += "~Triples\n";
		for (Entry<Integer, Query> entry : qset.entrySet()) {
			qsetAsString += "Query ID:" + entry.getKey() + " Triple: " + entry.getValue().getCQuery() + "\n";
		}
		
		qsetAsString += "~Restrictions" + "\n";
		qsetAsString += " -Attribute Restriction: ";
		
		if(restrictions != null ) qsetAsString += "'"+ restrictions.getInitialARestriction() + "'\n";
		else qsetAsString += "null"+ "\n";
		
		qsetAsString += " -Result Restriction: ";
		if(restrictions != null ) qsetAsString += "'" + restrictions.getInitialRRestriction() + "'\n";
		else qsetAsString += "null"+ "\n";
		
		return qsetAsString;
	}

	public String getQSetAsString() {
		String qsetstring = "\nQuery Set (QSet): \n";
		qsetstring += "~Triples\n";
		for (Entry<Integer, Query> entry : qset.entrySet()) {  
			qsetstring += "Query ID:" + entry.getKey();
			Tuple3<Grouping, Measuring, Operation> query = entry.getValue().getQueryTriple();
			qsetstring += ", Grouping Attributes: " + query._1().getGroupingAttributes().toString() + 
						  " Grouping Type: " + query._1().getGroupingType();
			qsetstring += ", Measuring Attribute: " + query._2().getMeasuringAttributes();
			qsetstring += ", Operation: " + query._3().getOperations();
			qsetstring += "\n";
		} 
		
		qsetstring += "~Restrictions" + "\n";
		qsetstring += " -Attribute Restriction: ";
		if(restrictions != null) qsetstring += "'" + restrictions.getARestrictionAsString() + "'\n";
		else qsetstring += "null" + "\n";
		
		qsetstring += " -Result Restriction: ";
		if(restrictions != null) qsetstring += "'" + restrictions.getRRestrictionAsString() + "'\n";
		else qsetstring += "null" + "\n";
		
		return qsetstring;
	}
	
	
	public Map<Integer, Query> getSubset(List<Integer> ids) {
		if(ids == null || this.qset == null) return null;
		else return this.qset
				 .entrySet() 
				 .stream() 
				 .filter(map -> ids.contains(map.getKey().intValue())) 
				 .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));  
	}

	public Restrictions getRestrictions() {
		return restrictions;
	}

	public HashMap<Integer, Query> getQSet(){
		return this.qset;
	}
	
	private boolean isValidSyntax(String command) {
		// -syntax checking, using a simple regex
		// -must be more complicated checking here...
		return Pattern.matches("\\s*\\(.*,.*,\\s*(min|max|count|sum|avg)\\s*\\)\\s*", command);
	}
	
	private boolean isARestrictionValid(String arestriction) {
		// ....
		return true;
	}
	
	public HashSet<String> getUsefulAttributes() {
		HashSet<String> attr = new HashSet<>();
		for (Entry<Integer, Query> entry : qset.entrySet()) {
			Tuple3<Grouping, Measuring, Operation> query = entry.getValue().getQueryTriple();
			attr.addAll(query._1().getUsefulAttributes());
			attr.addAll(query._2().getMeasuringAttributes()); 
		}
		return attr;
	}

}
