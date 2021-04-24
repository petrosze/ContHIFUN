package evaluator.sql.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import context.Context;
import evaluator.sql.RowCreation;
import query.QSet;
import query.Query;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class BatchSQLExecutionModelNonRewr {

	private Context context;
	private QSet qset;
	private Set<String> usefulAttributes;

	private ArrayList<String> datasets;

	public BatchSQLExecutionModelNonRewr(ArrayList<String> datasets_paths) {
		this.datasets = datasets_paths;
	}

	public void execute() throws InterruptedException {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder().appName("HiFun").master("local[*]").getOrCreate();

		Dataset<Row> df = spark.read().text(convertListToSeq(datasets));

		Dataset<Row> rows = df.as(Encoders.STRING()).flatMap(new RowCreation(context, usefulAttributes),
				createEncoder());
		
		Dataset<Row>[] ds_per_query = new Dataset[qset.getQSet().size()]; 
		
		for (int idxQ = 0; idxQ < qset.getQSet().size(); idxQ++) {
			Query query = getQueryByIndex(qset.getQSet(), idxQ);
			ds_per_query[idxQ] = rows.groupBy(query.getGroupingCols()).agg(query.getExprsMap());
		}
		
		for(int idxQ = 0; idxQ < ds_per_query.length; idxQ++) {
			ds_per_query[idxQ].show();
		}
		
		spark.close();
	}

	public void printInitializations() {
		System.out.println("~Batch SQL Execution Model~");
		System.out.println("------------");
		System.out.println("Query Set (RSet): " + this.qset.getQSetAsString());
		System.out.println("------------");
		System.out.println("Context Analysis \nSupporting Functions over Dataset: " + this.context.getFunctionsName());
		System.out.println("Useful Attributes including in RSet: " + this.usefulAttributes.toString());
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

	private ExpressionEncoder<Row> createEncoder() {
		StructType structType = new StructType();
		for (String attr : usefulAttributes) {
			structType = structType.add(attr, context.getTypes().get(attr), false);
		}
		return RowEncoder.apply(structType);
	}

	public Seq<String> convertListToSeq(List<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}
	
	private Query getQueryByIndex(Map<Integer, Query> map,int index){
	    return map.get( (map.keySet().toArray())[ index ] );
	}
}
