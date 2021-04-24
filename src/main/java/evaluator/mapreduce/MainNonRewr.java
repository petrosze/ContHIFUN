package evaluator.mapreduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import context.Context; 
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.mapreduce.execution.MapReduceExecutionModelNonRewr;
import query.QSet;

public class MainNonRewr {

	private static final Duration BATCH_DURATION = Durations.seconds(30); 
	// Path to HDFS
	private static final String INPUT_DIR = "hdfs://path/to/hdfs"; 
	
	public static void main(String[] args) 
			throws InterruptedException, StreamingQueryException, IOException{
		
		Context invoices = new Synthetic();
		InputQueries input_queries = new InputQueries();
		
		QSet qset = input_queries.getQsetCGMRR();
		//QSet qset = input_queries.getQsetCGRR();
		//QSet qset = input_queries.getQsetCMORR();
		//QSet qset = input_queries.getQsetBRR(); 
		qset.createTriples();
		
		HashSet<String> useful_attributes = qset.getUsefulAttributes();
		MapReduceExecutionModelNonRewr model = new MapReduceExecutionModelNonRewr(BATCH_DURATION, INPUT_DIR);
		model.setContext(invoices);
		model.setQSet(qset);
		model.setUsefulAttributes(useful_attributes);
		model.printInitializations();		
		model.execute();
	}
}
