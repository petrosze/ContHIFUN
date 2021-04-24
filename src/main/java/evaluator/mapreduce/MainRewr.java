package evaluator.mapreduce;
 
import java.io.IOException; 
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map; 
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import context.Context;
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.mapreduce.execution.MapReduceExecutionModelRewr;
import query.QSet;
import query.Query;
import query.RSet; 

public class MainRewr {

	private static final Duration BATCH_DURATION = Durations.seconds(60); 
	// Path to HDFS
	private static final String INPUT_DIR = "hdfs://path/to/hdfs"; 
	
	public static void main(String[] args) throws InterruptedException, IOException {
		
		Context context = new Synthetic();
		InputQueries input_queries = new InputQueries();
		
		QSet qset = input_queries.getQsetCGMRR();
		//QSet qset = input_queries.getQsetCGRR();
		//QSet qset = input_queries.getQsetCMORR();
		//QSet qset = input_queries.getQsetBRR(); 
		qset.createTriples();
		Map<Integer, Query> selectedQueries = qset.getSubset(Arrays.asList(1,2,3,4,5));
		RSet rset = new RSet(selectedQueries, qset.getRestrictions());
		
		
		if(rset.applyCommonGroupingMeasuringRule() == false) return;
		//if(rset.applyCommonGroupingRule() == false) return;
		//if(rset.applyCommonMeauringOperationRule() == false) return;
		//if(rset.applyBasicRewritingRule() == false) return;
		
		HashSet<String> useful_attributes = rset.getUsefulAttributes();
		MapReduceExecutionModelRewr model = new MapReduceExecutionModelRewr(BATCH_DURATION, INPUT_DIR);
		model.setContext(context);
		model.setRSet(rset);
		model.setUsefulAttributes(useful_attributes);
		model.printInitializations();		
		model.execute();
	}
}
