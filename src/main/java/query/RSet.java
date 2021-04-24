package query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.Map.Entry; 

import defines.GType;
import defines.RType;
import queryparts.Grouping;
import queryparts.Measuring;
import queryparts.Operation;
import queryparts.Restrictions;
import scala.Tuple3;

public class RSet implements Serializable{

	private Map<Integer, Query> qset;
	
	private Map<Integer, Query> rset;
	private Restrictions restrictions;
	private RType rtype;
	private int rqId;
	
	public RSet(Map<Integer, Query> qset, Restrictions restrictions) {
		this.qset = qset;
		this.restrictions = restrictions;
	}
	
	public RSet(Map<Integer, Query> qset) {
		this.qset = qset;
	}

	/**
	 * First Rewriting Rule (Same Grouping)
	 *  
	 * @param ids The id's of queries to apply the rewriting rule
	 *
	 * */
	public boolean applyCommonGroupingRule() {
	    
	   if(isApplicableSameGroupingRule() == false) {
	    	System.out.println("ERROR: Failure to apply Same Grouping Rewriting Rule");
	    	return false;
	   }
	   
	   this.rset = new HashMap<Integer, Query>();
	   this.rtype = RType.SAME_GROUPING_RULE;
	   this.rqId = 1;

	   Grouping gPart = this.qset.entrySet().iterator().next().getValue().getQueryTriple()._1();
	   
	   Measuring mPart = new Measuring();
	   Operation opPart = new Operation();
	   for(Map.Entry<Integer, Query> entry2 : this.qset.entrySet()){
			mPart.addMeasuring(entry2.getValue().getQueryTriple()._2());
		    opPart.addOperation(entry2.getValue().getQueryTriple()._3());
	   }
	   Tuple3<Grouping, Measuring, Operation> query = new Tuple3<>(gPart, mPart, opPart);
	   
	   rset.put(rqId, new Query(query));
	   return true;
	} 
	


	private boolean isApplicableSameGroupingRule() {
		
		if(this.qset == null) return false;
		
		Set<Grouping> allGroupings = new HashSet<>();
	    Set<Measuring> allMeasurings = new HashSet<>();
	    Set<Operation> allOperations = new HashSet<>();
	    
	    for(Map.Entry<Integer, Query> entry : this.qset.entrySet()){
	       Query query = entry.getValue();
	       allGroupings.add(query.getQueryTriple()._1());
	       allMeasurings.add(query.getQueryTriple()._2());
	       allOperations.add(query.getQueryTriple()._3());
	    }
	    return allGroupings.size() == 1 && (allMeasurings.size() == allOperations.size());
	}

	/**
	 * Second Rewriting Rule (Same Grouping, Measuring)
	 * 
	 * @param ids The id's of queries to apply the rewriting rule
	 *  
	 * */
	public boolean applyCommonGroupingMeasuringRule() {
		
	    if(isApplicableSameGroupingMeasuringRule() == false) {
	    	System.out.println("ERROR: Failure to apply Same Grouping Rewriting Rule");
	    	return false;
	    }
	    
		this.rset = new HashMap<Integer, Query>();
		this.rtype = RType.SAME_GROUPING_MEASURING_RULE;
		this.rqId = 1;
	    
		Grouping gPart = this.qset.entrySet().iterator().next().getValue().getQueryTriple()._1();
		Measuring mPart = this.qset.entrySet().iterator().next().getValue().getQueryTriple()._2();
		
		Operation opPart = new Operation();
		for(Map.Entry<Integer, Query> entry2 : this.qset.entrySet()){
			opPart.addOperation(entry2.getValue().getQueryTriple()._3());
		}
		Tuple3<Grouping, Measuring, Operation> query = new Tuple3<>(gPart, mPart, opPart);
		rset.put(rqId, new Query(query));
		return true;
	}
	
	private boolean isApplicableSameGroupingMeasuringRule() {
		
		if(this.qset == null) return false;
		
		Set<Grouping> allGroupings = new HashSet<>();
	    Set<Measuring> allMeasurings = new HashSet<>();
	    Set<Operation> allOperations = new HashSet<>();
	    
	    for(Map.Entry<Integer, Query> entry : this.qset.entrySet()){
	       Query query = entry.getValue();
	       allGroupings.add(query.getQueryTriple()._1());
	       allMeasurings.add(query.getQueryTriple()._2());
	       allOperations.add(query.getQueryTriple()._3());
	    }
	    return (allGroupings.size() == 1 && allMeasurings.size() == 1);
	}

	/**
	 * Cartesian Product Rewriting Rule (Same Measuring, Operation)
	 * 
	 * @param ids The id's of queries to apply the rewriting rule
	 *  
	 * */
	public boolean applyCommonMeauringOperationRule() {
	 
	    if(isApplicableCartesiaProductRule() == false) {
	    	System.out.println("ERROR: Failure to apply Cartesian Product Rewriting Rule");
	    	return false;
	    }
	    
	    this.rset = new HashMap<Integer, Query>();
	    this.rtype = RType.CARTESIAN_PRODUCT_RULE;
	    this.rqId = 1;
	    
	    
	    List<Grouping> allGroupings = new ArrayList<>();
	    for(Map.Entry<Integer, Query> entry : this.qset.entrySet()){
		       Query query = entry.getValue();
		       allGroupings.add(query.getQueryTriple()._1());
	    }
	   
		Grouping gPart = new Grouping(allGroupings);
		Measuring mPart = this.qset.entrySet().iterator().next().getValue().getQueryTriple()._2();;
	    Operation opPart = this.qset.entrySet().iterator().next().getValue().getQueryTriple()._3();

	    /* Add the abse query to rewrited list*/
		Tuple3<Grouping, Measuring, Operation> baseQuery = new Tuple3<>(gPart, mPart, opPart);
		rset.put(rqId, new Query(baseQuery));
	    
		/* Create projection queries */ 
		for(Grouping g : allGroupings) {
			Grouping projPart = new Grouping();
			projPart.setGrouping(g.getGroupingAttribute());
			projPart.setGroupingType(GType.PROJECTION_GROUPING);
			Tuple3<Grouping, Measuring, Operation> projectionQuery = new Tuple3<>(projPart, mPart, opPart);

			this.rqId++;
			rset.put(rqId, new Query(projectionQuery));	    
		}
		return true;
	}
	
	private boolean isApplicableCartesiaProductRule() {
		
		if(this.qset == null) return false;
		
		Set<Grouping> allGroupings = new HashSet<>();
	    Set<Measuring> allMeasurings = new HashSet<>();
	    Set<Operation> allOperations = new HashSet<>();
	    
	    for(Map.Entry<Integer, Query> entry : this.qset.entrySet()){
	       Query query = entry.getValue();
	       allGroupings.add(query.getQueryTriple()._1());
	       allMeasurings.add(query.getQueryTriple()._2());
	       allOperations.add(query.getQueryTriple()._3());
	    }	 
		return (allMeasurings.size() == 1 && allOperations.size() == 1) && 
				(allGroupings.size() == this.qset.size());
	
	}
	
	/**
	 * Basic Rewriting Rule
	 * 
	 * @param ids The id's of queries to apply the rewriting rule
	 *  
	 * */
	public boolean applyBasicRewritingRule() {
	
	    if(isApplicableBasicRewritingRule() == false) {
	    	System.out.println("ERROR: Failure to apply Same Basic Rewriting Rule");
	    	return false;
	    }
	    
	    this.rset = new HashMap<Integer, Query>();
	    this.rtype = RType.BASIC_REWRITING_RULE;
	 
	    Iterator<Entry<Integer, Query>> iterator = this.qset.entrySet().iterator();
		Tuple3<Grouping, Measuring, Operation> query = iterator.next().getValue().getQueryTriple();
		
		/* Add the base query to rewrited list*/
		Tuple3<Grouping, Measuring, Operation> baseq = 
				new Tuple3<>(
						new Grouping(query._1().getGroupingAttributes().get(1)), 
						query._2(), 
						query._3());
		
		this.rqId = 1;
		rset.put(rqId, new Query(baseq));
		
		for(Map.Entry<Integer, Query> entry : this.qset.entrySet()){
			Query q = entry.getValue();
			
			Tuple3<Grouping, Measuring, Operation> newQuery = 
				new Tuple3<>(
						new Grouping(q.getQueryTriple()._1().getFirstGroupingAttribute()), 
						new Measuring("ansQID_" + 1),
						q.getQueryTriple()._3());
			
				this.rqId++;
				rset.put(rqId, new Query(newQuery));
		}
		return true;
	}
	
	private boolean isApplicableBasicRewritingRule() {

		System.out.println(this.qset.toString());
		
		if(this.qset == null) return false;
		
		Set<Grouping> allGroupings = new HashSet<>();
	    Set<Measuring> allMeasurings = new HashSet<>();
	    Set<Operation> allOperations = new HashSet<>();
	    
	    for(Map.Entry<Integer, Query> entry : this.qset.entrySet()){
	    	Query query = entry.getValue();
	    	allGroupings.add(query.getQueryTriple()._1());
	    	allMeasurings.add(query.getQueryTriple()._2());
	    	allOperations.add(query.getQueryTriple()._3());
		}	
	    
	    Set<String> firstGrouping = new HashSet<>();
	    Set<String> secondGrouping = new HashSet<>();
	    for(Grouping g : allGroupings){
	    	firstGrouping.add(g.getFirstGroupingAttribute());
	    	secondGrouping.add(g.getSecondGroupingAttribute());
	    }
	    
	    return (allMeasurings.size() == 1 && allOperations.size() == 1) && 
	    		(firstGrouping.size() == this.qset.size()) &&
	    		secondGrouping.size() == 1;
	}

	public String getRSetAsString() {
		
		if(this.rset == null) 
			return "Rewriting Set (RSet) is empty";
		
		String rsetstring = "\nRewrited Set (RSet)\n";
		rsetstring += "Rewriting Rule: " + this.rtype + "\n";
		for(Entry<Integer, Query> entry : rset.entrySet()) {
			rsetstring += "Query ID:" + entry.getKey();
			Tuple3<Grouping, Measuring, Operation> query = entry.getValue().getQueryTriple();
			rsetstring += ", Grouping Attributes: " + query._1().getGroupingAttributes().toString() + 
					  " Grouping Type: " + query._1().getGroupingType();
			rsetstring += ", Measuring Attribute: " + query._2().getMeasuringAttributes().toString();
			rsetstring += ", Operation: " + query._3().getOperations() + "\n";
		}
		//rsetstring += "\n";
		return rsetstring;
	}

	public Restrictions getRestrictions() {
		return restrictions;
	}

	public RType getRType() {
		return rtype;
	}

	public String getARestrictionAsString() {
		return this.restrictions.getARestrictionAsString();
	}
	

	public String getARString() {
		return this.restrictions.getARString();
	}
	
	public boolean containsARestriction() {
		if(this.restrictions != null) {
			if(this.restrictions.containsARestriction()) return true;
		} 
		return false;
	}
	
	public Query getFirstQuery() {
		if(rset != null) {
			Entry<Integer, Query> entry = rset.entrySet().iterator().next();
			return entry.getValue();
		}
		else return null;
	}

	public Map<Integer, Query> getRSet() {
		return this.rset;
	}

	/**
	 * Returns all including attributes on rewritten query. All attributes is available on 
	 * the first query in the case of Basic Rewriting and Cartesian Product rewriting rule. 
	 * */
	public HashSet<String> getUsefulAttributes() {
		 
		HashSet<String> attr = new HashSet<>();
		Query firstQ = this.rset.get(1); // get first query on rewriting set 'rset'.
		
		Grouping gPart = firstQ.getQueryTriple()._1();
	    attr.addAll(gPart.getUsefulAttributes());
	    
	    Measuring mPart = firstQ.getQueryTriple()._2();
	    attr.addAll(mPart.getMeasuringAttributes());
	    
		return attr;
	}	
}
