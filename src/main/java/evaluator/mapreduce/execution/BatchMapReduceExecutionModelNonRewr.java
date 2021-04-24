package evaluator.mapreduce.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import context.Context; 
import evaluator.mapreduce.conceptualschema.InputPreparation;
import evaluator.mapreduce.conceptualschema.KVConstruction;
import evaluator.mapreduce.conceptualschema.Key; 
import evaluator.mapreduce.conceptualschema.Reduction;
import evaluator.mapreduce.conceptualschema.Tuple;
import evaluator.mapreduce.measures.MValue;
import query.QSet;
import query.Query; 

public class BatchMapReduceExecutionModelNonRewr {

	private Context context;
	private QSet qset;
	private Set<String> usefulAttributes;

	private ArrayList<String> datasets;

	public BatchMapReduceExecutionModelNonRewr(ArrayList<String> datasetsPaths) {
		this.datasets = datasetsPaths;
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
		Broadcast<QSet> qset = sc.broadcast(this.qset);

		List<JavaRDD<String>> rdds_l = new ArrayList<JavaRDD<String>>();
		for (int idx = 0; idx < datasets.size(); idx++) {
			rdds_l.add(sc.textFile(datasets.get(idx)));
		}
		JavaRDD<String> firstRDD = rdds_l.get(0);
		List<JavaRDD<String>> restRDDs = rdds_l.subList(1, rdds_l.size());
		JavaRDD<String> input = sc.union(firstRDD, restRDDs);

		JavaRDD<Tuple> tuples = input.map(new InputPreparation(context.getValue(), useful_attributes.getValue()));
		//printTupleRDDContent(tuples);
		
		Map<Integer, Query> set = qset.getValue().getQSet();
		for(int idxQ = 0; idxQ < set.size(); idxQ++) {

			Query query = getQueryByIndex(set, idxQ);
			Broadcast<Query> bquery = sc.broadcast(query);
			
			JavaPairRDD<Key, ArrayList<MValue>> constructedPairs = tuples
					.mapToPair(new KVConstruction(bquery.getValue()));		

			JavaPairRDD<Key, ArrayList<MValue>> reductedPairs = constructedPairs
					.reduceByKey(new Reduction(bquery.getValue()));
		
			triggerExecution(reductedPairs);
		}
		sc.close();
	}

	public void setContext(Context cnxt) {
		this.context = cnxt;
	}

	public void setQSet(QSet qset) {
		this.qset = qset;
	}

	public void setUsefulAttributes(HashSet<String> useful_attributes) {
		this.usefulAttributes = new HashSet<String>();
		this.usefulAttributes = useful_attributes;
	}

	public void printInitializations() {
		System.out.println("~Batch MapReduce Execution Model~");
		System.out.println(" (queries execution without rewriting)");
		System.out.println("------------\n" + "Query Set (RSet): " + this.qset.getQSetAsString() + "------------");
		System.out.println("List of Input Files: " + this.datasets.toString());
		System.out.println("Context Analysis \nSupporting Functions over Dataset: " + this.context.getFunctionsName());
		System.out.println("Useful Attributes including in RSet: " + this.usefulAttributes.toString());
	}

	private Query getQueryByIndex(Map<Integer, Query> map, int index) {
		return map.get((map.keySet().toArray())[index]);
	}

	private void triggerExecution(JavaPairRDD<Key, ArrayList<MValue>> rdd) {
		rdd.take(5).forEach(f -> {
			System.out.println(f._1.toString() + " " + f._2.toString());
		});
	}
	
	private void printPairRDDContent(JavaPairRDD<Key, ArrayList<MValue>> rdd) {
		rdd.collect().forEach(f -> {
			System.out.println(f._1.toString() + " " + f._2.toString());
		});
	}
	
	private void printTupleRDDContent(JavaRDD<Tuple> rdd) {
		rdd.collect().forEach(f -> {
			System.out.println(f.toString());
		});
	}
}
