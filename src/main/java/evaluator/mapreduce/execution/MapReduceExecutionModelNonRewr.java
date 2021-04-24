package evaluator.mapreduce.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream; 
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import context.Context;
import evaluator.mapreduce.conceptualschema.IcrUpdate;
import evaluator.mapreduce.conceptualschema.InputPreparation;
import evaluator.mapreduce.conceptualschema.KVConstruction;
import evaluator.mapreduce.conceptualschema.Key;
import evaluator.mapreduce.conceptualschema.Reduction;
import evaluator.mapreduce.conceptualschema.Tuple;
import evaluator.mapreduce.measures.MValue;
import query.QSet;
import query.Query; 
import scala.Tuple2;
import utils.StreamSimulator;

public class MapReduceExecutionModelNonRewr {

	private Duration BATCH_DURATION;
	private String INPUT_DIR;
	private String CHECKPOINT_DIR;
	
	private Context context; 
	private QSet qset;
	private Set<String> usefulAttributes;
	
	private static final boolean ONE_AT_TIME = true;
	 
	public MapReduceExecutionModelNonRewr(Duration _BATCH_DURATION, String _INPUT_DIR) {
		this.INPUT_DIR = _INPUT_DIR;
		this.BATCH_DURATION = _BATCH_DURATION;
		this.CHECKPOINT_DIR = "/tmp";
	}
	
	/**
	 * Dynamic number of separated queries are executed using map-reduce model. 
	 * ~ Rewriting does not applied. 
	 *
	 * */
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
		Broadcast<QSet> qset = ssc.sparkContext().broadcast(this.qset);
		
		StreamSimulator stream = new StreamSimulator(INPUT_DIR, ssc);
		JavaDStream<String> lines = ssc.queueStream(stream.getQueueOfRDDs(), ONE_AT_TIME);
		//lines.print();
		
		JavaDStream<Tuple> tuples = lines.map(new InputPreparation(context.getValue(), useful_attributes.getValue()));
		tuples.print();
		
		Map<Integer, Query> set = qset.getValue().getQSet();
		
		ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>> state_l =
				new ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>>();

		for(int idxQ = 0; idxQ < set.size(); idxQ++) {
			
			Query query = getQueryByIndex(set, idxQ);
			Broadcast<Query> bquery = ssc.sparkContext().broadcast(query);
			
			JavaPairDStream<Key, ArrayList<MValue>> constructedPairs = tuples
					.mapToPair(new KVConstruction(bquery.getValue()));
			//constructedPairs.print();
			
			
			JavaPairDStream<Key, ArrayList<MValue>> reductedPairs = constructedPairs
					.reduceByKey(new Reduction(bquery.getValue()));
			reductedPairs.print();
			
			state_l.add(idxQ, reductedPairs.mapWithState(StateSpec.function(new IcrUpdate(bquery.getValue()))));	
		}
		
		//triggerExecution(state_l);
		
		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	} 

	public void setContext(Context invoices) {
		this.context = invoices;
	}

	public void setQSet(QSet qset) {
		this.qset = qset;
	}
	
	public void setUsefulAttributes(HashSet<String> useful_attributes) {
		this.usefulAttributes = new HashSet<String>();
		this.usefulAttributes = useful_attributes;
	}

	public void printInitializations() {
		System.out.println("~MapReduce Execution Model~");
		System.out.println(" (queries execution without rewriting)");
		System.out.println("Batch Duration: " + this.BATCH_DURATION + "");
		System.out.println("Input Directory: " + this.INPUT_DIR);
		System.out.println("------------" + this.qset.getQSetAsString() + "------------");
		System.out.println("Context Analysis \nSupporting Functions over Dataset: " + this.context.getFunctionsName());
		System.out.println("Useful Attributes including in QSet: " + this.usefulAttributes.toString());
	}
	
	private Query getQueryByIndex(Map<Integer, Query> map,int index){
	    return map.get( (map.keySet().toArray())[ index ] );
	}
	
	private void triggerExecution(ArrayList<JavaMapWithStateDStream<Key, ArrayList<MValue>, ArrayList<MValue>, Tuple2<Key, ArrayList<MValue>>>> state_l) {
		System.out.println("----------------");
		for(int index =0; index<state_l.size(); index++) {
			state_l.get(index).print();
		}
		System.out.println("----------------");
	}
}
