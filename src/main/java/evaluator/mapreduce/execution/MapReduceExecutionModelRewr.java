package evaluator.mapreduce.execution;
 
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import context.Context;
import defines.RType;
import evaluator.mapreduce.conceptualschema.AKVConstruction;
import evaluator.mapreduce.conceptualschema.Filtering;
import evaluator.mapreduce.conceptualschema.IcrUpdate;
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
import utils.StreamSimulator;

public class MapReduceExecutionModelRewr implements Serializable{

	private Duration BATCH_DURATION;
	private String INPUT_DIR;
	private String CHECKPOINT_DIR;
	
	private Context context; 
	private RSet rset;
	private Set<String> usefulAttributes;
	 
	private static final boolean ONE_AT_TIME = true;
	
	public MapReduceExecutionModelRewr(Duration _BATCH_DURATION, String _INPUT_DIR) {
		this.INPUT_DIR = _INPUT_DIR;
		this.BATCH_DURATION = _BATCH_DURATION;
		this.CHECKPOINT_DIR = "/tmp";
	}


	public void execute() throws InterruptedException, IOException {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("SHiFun");
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, BATCH_DURATION);
		ssc.checkpoint(CHECKPOINT_DIR);

		Broadcast<Context> context = ssc.sparkContext().broadcast(this.context);
		Broadcast<Set<String>> useful_attributes = ssc.sparkContext().broadcast(this.usefulAttributes);
 		Broadcast<RSet> rset = ssc.sparkContext().broadcast(this.rset);
		
		StreamSimulator stream = new StreamSimulator(INPUT_DIR, ssc);
		JavaDStream<String> lines = ssc.queueStream(stream.getQueueOfRDDs(), ONE_AT_TIME);
		//lines.print();
		
		JavaDStream<Tuple> tuples = lines.map(new InputPreparation(context.getValue(), useful_attributes.getValue()));
		tuples.print();
		
		if (rset.getValue().containsARestriction()) {
			tuples = tuples.filter(new Filtering(rset.getValue().getRestrictions().getACondition()));
		}
		
		JavaPairDStream<Key, ArrayList<MValue>> constructedPairs = tuples
				.mapToPair(new KVConstruction(rset.getValue().getFirstQuery()));
		//constructedPairs.print();
		
		JavaPairDStream<Key, ArrayList<MValue>> reductedPairs = constructedPairs
				.reduceByKey(new Reduction(rset.getValue().getFirstQuery()));
		//reductedPairs.print();
		
		ArrayList<JavaPairDStream<Key, ArrayList<MValue>>> projReducted_l = null;
		JavaPairDStream<Key, ArrayList<MValue>> assKVReducted = null;
		
		if(rset.getValue().getRType() == RType.BASIC_REWRITING_RULE) {
			
			projReducted_l = new ArrayList<JavaPairDStream<Key, ArrayList<MValue>>>();
			Map<Integer, Query> set = rset.getValue().getRSet();
			
			for(int idxQ = 1; idxQ < set.size(); idxQ++) {
				
				Query query = getQueryByIndex(set, idxQ);
				Broadcast<Query> bquery = ssc.sparkContext().broadcast(query);
				
				JavaPairDStream<Key, ArrayList<MValue>> assocKVPairs = reductedPairs
						.mapToPair(new AKVConstruction(bquery.getValue(), context.getValue()));
				//assocKVPairs.print();
				
				assKVReducted = assocKVPairs
						.reduceByKey(new Reduction(bquery.getValue()));
				//assKVReducted.print();
				
				projReducted_l.add(assKVReducted);
			}
			
			Map<Integer, Query> set = rset.getValue().getRSet();
			Query secondQ = getQueryByIndex(set, 1);
			Broadcast<Query> query = ssc.sparkContext().broadcast(secondQ);
		
			JavaPairDStream<Key, ArrayList<MValue>> assocKVPairs = reductedPairs
					.mapToPair(new AKVConstruction(query.getValue(), context.getValue()));
			//assocKVPairs.print();
			
			assKVReducted = assocKVPairs
					.reduceByKey(new Reduction(query.getValue()));
			//assKVReducted.print();
		} 
		else if(rset.getValue().getRType() == RType.CARTESIAN_PRODUCT_RULE) {
			
			projReducted_l = new ArrayList<JavaPairDStream<Key, ArrayList<MValue>>>();
			Map<Integer, Query> set = rset.getValue().getRSet();
			
			for(int idxQ = 1; idxQ < set.size(); idxQ++) {
				Query query = getQueryByIndex(set, idxQ);
				
				Broadcast<Query> pquery = ssc.sparkContext().broadcast(query);
				
				JavaPairDStream<Key, ArrayList<MValue>> projKVPairs = reductedPairs
						.mapToPair(new PKVConstruction(pquery.getValue()));
				
				JavaPairDStream<Key, ArrayList<MValue>> projKVReducted = projKVPairs
						.reduceByKey(new Reduction(pquery.getValue()));
				
				//projKVReducted.print();
				
				projReducted_l.add(projKVReducted);
			}
		}
		
		// Incrementalization
		if(rset.getValue().getRType() == RType.BASIC_REWRITING_RULE) {
			
			ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>> state_l = 
					new ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>>();
			
			Map<Integer, Query> set = rset.getValue().getRSet();
			int index = 0;
			
			for(int idxQ = 1; idxQ < set.size(); idxQ++) {
				
				Query query = getQueryByIndex(set, idxQ);
				Broadcast<Query> pquery = ssc.sparkContext().broadcast(query);
				
				state_l.add(index, projReducted_l.get(index).mapWithState(StateSpec.function(new IcrUpdate(pquery.getValue()))));
				index++;
			}
			triggerExecutionL(state_l);
			
			JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>> state = assKVReducted
					.mapWithState(StateSpec.function(new IcrUpdate(rset.getValue().getFirstQuery())));
			//state.print();
			triggerExecution(state);
		}
		else 
		if(rset.getValue().getRType() == RType.SAME_GROUPING_RULE || rset.getValue().getRType() == RType.SAME_GROUPING_MEASURING_RULE) {
		
			JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>> state = reductedPairs
					.mapWithState(StateSpec.function(new IcrUpdate(rset.getValue().getFirstQuery())));
			state.print();
			
			//triggerExecution(state);
		}
		else if(rset.getValue().getRType() == RType.CARTESIAN_PRODUCT_RULE) {
			
			ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>> state_l = 
					new ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>>();
			
			Map<Integer, Query> set = rset.getValue().getRSet();
			int index = 0;
			
			for(int idxQ = 1; idxQ < set.size(); idxQ++) {
				
				Query query = getQueryByIndex(set, idxQ);
				Broadcast<Query> pquery = ssc.sparkContext().broadcast(query);
				
				state_l.add(index, projReducted_l.get(index).mapWithState(StateSpec.function(new IcrUpdate(pquery.getValue()))));
				index++;
			}
			triggerExecutionL(state_l);
		}
		
		ssc.start();
		ssc.awaitTermination();
		ssc.close();
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
		System.out.println("~MapReduce Execution Model~");
		System.out.println(" (queries execution with rewritings)");
		System.out.println("Batch Duration: " + this.BATCH_DURATION + "");
		System.out.println("Input Directory: " + this.INPUT_DIR);
		System.out.println("------------" + this.rset.getRSetAsString() + "------------");
		System.out.println("Context Analysis \nSupporting Functions over Dataset: " + this.context.getFunctionsName());
		System.out.println("Useful Attributes including in QSet: " + this.usefulAttributes.toString());
	}
	
	private void printTuples(JavaDStream<Tuple> tuples) {
		tuples.foreachRDD((newEventsRdd, time) -> {
		System.out.println("\n===================================");
			System.out.println("New Events for " + time + " batch:");
			for (Tuple tuple : newEventsRdd.collect()) {
				System.out.println(tuple.toString());
			}
		});
	}
	
	private void printPairs(JavaPairDStream<Key, ArrayList<MValue>> pairs) {
		pairs.foreachRDD((newEventsRdd, time) -> {
		System.out.println("\n===================================");
		System.out.println("New Events for " + time + " batch:");
			for (Tuple2<Key, ArrayList<MValue>> tuple : newEventsRdd.collect()) {
				System.out.println("Key: " + tuple._1.toString() + " Value: " +  tuple._2.toString());
			}
		});
	}

	private void printState(JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>> stateDstream) {
		stateDstream.stateSnapshots().foreachRDD((newEventsRdd, time) -> {
			System.out.println("\n========Aggregated Results=============");
			System.out.println("New Events for " + time + " batch:");
			for (Tuple2<Key, ArrayList<MValue>> tuple : newEventsRdd.collect()) {
				System.out.println(tuple._1.toString() + ", " + tuple._2.toString());
			}
		}); 
	}
	
	private void triggerExecution(JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>> stateDstream) {
		stateDstream.stateSnapshots().foreachRDD((newEventsRdd, time) -> {
			System.out.println(newEventsRdd.count());

		}); 
	}
	
	private void triggerExecutionL(ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>> state_l) {
		for(int index =0; index<state_l.size(); index++) {
			state_l.get(index).print();
		}
	}

	private Query getQueryByIndex(Map<Integer, Query> map,int index){
	    return map.get( (map.keySet().toArray())[ index ] );
	}
}
