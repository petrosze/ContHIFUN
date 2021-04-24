package evaluator.mapreduce.execution;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast; 

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set; 
import org.apache.log4j.Level;

import context.Context;
import defines.RType;
import evaluator.mapreduce.conceptualschema.AKVConstruction;
import evaluator.mapreduce.conceptualschema.InputPreparation;
import evaluator.mapreduce.conceptualschema.KVConstruction;
import evaluator.mapreduce.conceptualschema.Key;
import evaluator.mapreduce.conceptualschema.PKVConstruction;
import evaluator.mapreduce.conceptualschema.Reduction;
import evaluator.mapreduce.conceptualschema.Tuple;
import evaluator.mapreduce.measures.MValue;
import query.Query;
import query.RSet;
import scala.Tuple2;

public class BatchMapReduceExecutionModelRewr {

	private Context context;
	private RSet rset;
	private Set<String> usefulAttributes;
	
	private ArrayList<String> datasets;
	
	public BatchMapReduceExecutionModelRewr(ArrayList<String> datasets) {
		 this.datasets = datasets;
	}

	public void execute() throws InterruptedException {
		
		Logger.getLogger("org").setLevel(Level.OFF);
	    Logger.getLogger("akka").setLevel(Level.OFF);
 
	    SparkConf conf = new SparkConf()
	    		.setAppName("Batch MapReduce Execution Model")
	    		.setMaster("local[*]");
	     
	    JavaSparkContext sc = new JavaSparkContext(conf);
	     
	    
	 	Broadcast<Context> context = sc.broadcast(this.context);
	 	Broadcast<Set<String>> useful_attributes = sc.broadcast(this.usefulAttributes);
		Broadcast<RSet> rset = sc.broadcast(this.rset);
	    
	    List<JavaRDD<String>> rdds_l = new ArrayList<JavaRDD<String>>();
	    
	    for(int idx = 0; idx < datasets.size() ; idx++) {
	    	rdds_l.add(sc.textFile(datasets.get(idx))); 
	    }
	    
	    JavaRDD<String> firstRDD = rdds_l.get(0);
	    List<JavaRDD<String>> restRDDs = rdds_l.subList(1, rdds_l.size());
	    JavaRDD<String> input = sc.union(firstRDD, restRDDs);
	     
	    JavaRDD<Tuple> tuples = input.map(new InputPreparation(context.getValue(), useful_attributes.getValue()));

		JavaPairRDD<Key, ArrayList<MValue>> constructedPairs = tuples
				.mapToPair(new KVConstruction(rset.getValue().getFirstQuery()));		

		JavaPairRDD<Key, ArrayList<MValue>> reductedPairs = constructedPairs
				.reduceByKey(new Reduction(rset.getValue().getFirstQuery()));
			

		if(rset.getValue().getRType() == RType.SAME_GROUPING_MEASURING_RULE || 
				rset.getValue().getRType() == RType.SAME_GROUPING_RULE) {

			triggerExecution(reductedPairs);
		}
		else if(rset.getValue().getRType() == RType.BASIC_REWRITING_RULE) {
	 		
	 		Map<Integer, Query> set = rset.getValue().getRSet();
			Query secondQ = getQueryByIndex(set, 1);
			Broadcast<Query> query = sc.broadcast(secondQ);
		
			JavaPairRDD<Key, ArrayList<MValue>> assocKVPairs = reductedPairs
					.mapToPair(new AKVConstruction(query.getValue(), context.getValue()));
	
			JavaPairRDD<Key, ArrayList<MValue>> assKVReducted = assocKVPairs
					.reduceByKey(new Reduction(query.getValue()));
			
			triggerExecution(assKVReducted);
	 	}
		else if(rset.getValue().getRType() == RType.CARTESIAN_PRODUCT_RULE) {
			
			Map<Integer, Query> set = rset.getValue().getRSet();
			for(int idxQ = 1; idxQ < set.size(); idxQ++) {
				
				Query query = getQueryByIndex(set, idxQ);
				
				Broadcast<Query> pquery = sc.broadcast(query);
				
				JavaPairRDD<Key, ArrayList<MValue>> projKVPairs = reductedPairs
						.mapToPair(new PKVConstruction(pquery.getValue()));
				
				JavaPairRDD<Key, ArrayList<MValue>> projKVReducted = projKVPairs
						.reduceByKey(new Reduction(pquery.getValue()));
			
			//	triggerExecution(projKVReducted);
			}
		}
	    sc.close();		
	}


	public void setContext(Context invoices) {
		this.context = invoices;
	}

	public void setRSet(RSet rset) {
		this.rset = rset;
	}

	public void setUsefulAttributes(HashSet<String> useful_attributes) {
		this.usefulAttributes = new HashSet<String>();
		this.usefulAttributes = useful_attributes;
	}

	public void printInitializations() {
		System.out.println("~Batch MapReduce Execution Model~");
		System.out.println("------------\n" + "Query Set (RSet): " + this.rset.getRSetAsString() + "------------");
		System.out.println("List of Input Files: " + this.datasets.toString());
		System.out.println("Context Analysis \nSupporting Functions over Dataset: " + this.context.getFunctionsName());
		System.out.println("Useful Attributes including in RSet: " + this.usefulAttributes.toString());
	}

	private Query getQueryByIndex(Map<Integer, Query> map,int index){
	    return map.get( (map.keySet().toArray())[ index ] );
	}
	
	private void triggerExecution(JavaPairRDD<Key, ArrayList<MValue>> rdd) {
		 rdd.take(5).forEach(f ->{
			 System.out.println(f._1.toString() + " " + f._2.toString());
		 });
	}
}
