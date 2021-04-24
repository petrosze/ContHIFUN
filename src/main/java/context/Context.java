package context;
 
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/*
 * This abstract class implemented for each individual 
 * input file. Specifies the functions (direct and derived) 
 * which including in the 'Analysis Context'
 * 
 * */

public abstract class Context implements Serializable{

	protected Map<String, Function<Object, Object>> functions;
	protected Map<String, DataType> types;
	protected StructType schema;
	
	protected abstract void defineFunctions();
	protected abstract void defineTypes();
	
	protected abstract void defineSchema();

	public Context(){
		this.functions = new HashMap<String, Function<Object , Object>>();
		defineFunctions();
		defineTypes();
		defineSchema();
	}

	public Map<String, Function<Object, Object>> getFunctions(){
		return this.functions;
	}
	 
	public List<String> getFunctionsName(){
		if(this.functions == null ) return null;
		else return new ArrayList<String>(this.functions.keySet());
	}
	
	public  Map<String, DataType> getTypes(){
		return this.types;
	}
	
	public StructType getSchema() {
		return this.schema;
	}
}
