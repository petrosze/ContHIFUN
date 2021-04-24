package evaluator.mapreduce;

import java.io.FileNotFoundException;
import java.util.ArrayList; 
import java.util.HashSet;
 

import context.Context;
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.mapreduce.execution.BatchMapReduceExecutionModelNonRewr;
import query.QSet;
import utils.ReadPathsFromLocalFile;

public class MainBatchNonRewr {

	private static ArrayList<String> datasets_paths = null; 
	
	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
		
		datasets_paths = new ArrayList<String>(); 
		if(datasets_paths == null) 
			datasets_paths = new ReadPathsFromLocalFile().getPaths();
		
		Context context = new Synthetic();
		
		InputQueries input_queries = new InputQueries();
		
		QSet qset = input_queries.getQsetCGMRR();
		//QSet qset = input_queries.getQsetCGRR();
		//QSet qset = input_queries.getQsetCMORR();
		//QSet qset = input_queries.getQsetBRR(); 
		qset.createTriples();
		 
		
		HashSet<String> useful_attributes = qset.getUsefulAttributes();
		BatchMapReduceExecutionModelNonRewr model = new BatchMapReduceExecutionModelNonRewr(datasets_paths);
		model.setContext(context);
		model.setQSet(qset);
		model.setUsefulAttributes(useful_attributes);
		model.printInitializations(); 
		model.execute();
	}

}
