package evaluator.sql.execution;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger; 

import java.util.Map;

import org.apache.log4j.Level;

import context.Context;
import query.QSet;
import query.Query;

public class SQLExecutionModelNonRewr {

	private String TRIGGER_INTERVAL;
	private String INPUT_FOLDER;
	
	private Context context; 
	private QSet qset;
	
	public SQLExecutionModelNonRewr(String _TRIGGER_INTERVAL, String _INPUT_FOLDER) {
		this.TRIGGER_INTERVAL = _TRIGGER_INTERVAL;
		this.INPUT_FOLDER = _INPUT_FOLDER;
	}
	
	/**
	 * Dynamic number of separated queries are executed using SQL semantics. 
	 * ~ Rewriting does not applied. 
	 *
	 * */
	
	public void execute() throws StreamingQueryException {
	
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("HiFun")
				.getOrCreate();
		
		Dataset<Row> df = spark
				.readStream()
				.option("header", "false")
			    .option("maxFilesPerTrigger",1)
				.schema(context.getSchema())
				.csv(INPUT_FOLDER);
		
		Map<Integer, Query> set = qset.getQSet();
		
		Dataset<Row>[] df_per_query = new Dataset[set.size()]; 
		
		for(int idxQ = 0; idxQ < set.size(); idxQ++) {
			Query query = getQueryByIndex(set, idxQ);
			df_per_query[idxQ] = df.groupBy(query.getGroupingCols()).agg(query.getExprsMap());
		}
		
		StreamingQuery[] queries = new StreamingQuery[set.size()]; 
		
		for(int idxQ = 0; idxQ < df_per_query.length; idxQ++) {
			queries[idxQ] = df_per_query[idxQ]
					.writeStream()
					.outputMode(OutputMode.Update())
					.format("console")
					.trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL))
					.start();
		}
		
		for(int idxQ = 0; idxQ < queries.length; idxQ++) {
			queries[idxQ].awaitTermination();
		}
	}
	
	public void setQSet(QSet qset) {
		this.qset = qset;
	}
	
	public void setContext(Context context) {
		this.context = context;
	}
	
	public void printInitializations() {
		System.out.println("~MapReduce Execution Model~");
		System.out.println("Trigger Interval: " + this.TRIGGER_INTERVAL + "");
		System.out.println("------------");
		System.out.println("Query Set (RSet): " + this.qset.getQSetAsString());
		System.out.println("------------");
	}
	
	private Query getQueryByIndex(Map<Integer, Query> map,int index){
	    return map.get( (map.keySet().toArray())[ index ] );
	}
}
