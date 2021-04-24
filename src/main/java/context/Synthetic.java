package context;

import java.io.Serializable;
import java.util.HashMap;
import java.util.function.Function;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
 

public class Synthetic extends Context{

	public Synthetic() {
		super();
	}
	
	@Override
	protected void defineFunctions() {
		
		functions.put("g1", new g1());
		functions.put("g2", new g2());
		functions.put("g3", new g3());
		functions.put("g4", new g4());
		functions.put("g5", new g5());
		
		// *
		functions.put("g11*g1", new g11_comp_g1());
		functions.put("g11", new g11());
	
		functions.put("g12*g1", new g12_comp_g1());
		functions.put("g12", new g12());
		
		functions.put("g13*g1", new g13_comp_g1());
		functions.put("g13", new g13());
		
		functions.put("g14*g1", new g14_comp_g1());
		functions.put("g14", new g14());
		
		functions.put("g15*g1", new g15_comp_g1());
		functions.put("g15", new g15());
		
		functions.put("m1", new m1());
		functions.put("m2", new m2());
		functions.put("m3", new m3());
		functions.put("m4", new m4());
		functions.put("m5", new m5());
		functions.put("m6", new m6());
		functions.put("m7", new m7());
		functions.put("m8", new m8());
		functions.put("m9", new m9());
		functions.put("m10", new m10());
		
	}

	@Override
	protected void defineTypes() {
		
		types = new HashMap<String, DataType>();
		
		types.put("g1", DataTypes.StringType);
		types.put("g2", DataTypes.StringType);
		types.put("g3", DataTypes.StringType);
		types.put("g4", DataTypes.StringType);
		types.put("g5",	DataTypes.StringType); 
		
		types.put("g11*g1",	DataTypes.StringType); 
		types.put("g11",	DataTypes.StringType);  
		
		types.put("g12*g1",	DataTypes.StringType); 
		types.put("g12",	DataTypes.StringType);  
		
		types.put("g13*g1",	DataTypes.StringType); 
		types.put("g13",	DataTypes.StringType);  
		
		types.put("g14*g1",	DataTypes.StringType); 
		types.put("g14",	DataTypes.StringType);  
		
		types.put("g15*g1",	DataTypes.StringType); 
		types.put("g15",	DataTypes.StringType);  
		
		
		types.put("m1",  DataTypes.IntegerType); 
		types.put("m2",  DataTypes.IntegerType); 
		types.put("m3",  DataTypes.IntegerType); 
		types.put("m4",  DataTypes.IntegerType); 
		types.put("m5",  DataTypes.IntegerType); 
		types.put("m6",  DataTypes.IntegerType); 
		types.put("m7",  DataTypes.IntegerType); 
		types.put("m8",  DataTypes.IntegerType);
		types.put("m9",  DataTypes.IntegerType); 
		types.put("m10", DataTypes.IntegerType); 
	}
	
	@Override
	protected void defineSchema() {
		
		StructType schema = new StructType()
				.add("g1", "string")
				.add("g2", "string")
				.add("g3", "string")
				.add("g4", "string")
				.add("g5", "string")
				.add("m1", "integer")
				.add("m2", "integer")
				.add("m3", "integer")
				.add("m4", "integer")
				.add("m5", "integer")
				.add("m6", "integer")
				.add("m7", "integer")
				.add("m8", "integer")
				.add("m9", "integer")
				.add("m10", "integer");
		
		this.schema = schema;
	}
	
	class g1 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return new String(cline.split(",")[0]);
		}
	}
	
	class g2 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return new String(cline.split(",")[1]);
		}
	}
	
	class g3 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return new String(cline.split(",")[2]);
		}
	}
	
	class g4 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return new String(cline.split(",")[3]);
		}
	}
	
	class g5 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return new String(cline.split(",")[4]);
		}
	}
	 
	class g11_comp_g1 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			String direct_value = cline.split(",")[0];
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g11 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String direct_value = (String) line;
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g12_comp_g1 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			String direct_value = cline.split(",")[0];
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g12 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String direct_value = (String) line;
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g13_comp_g1 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			String direct_value = cline.split(",")[0];
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g13 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String direct_value = (String) line;
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g14_comp_g1 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			String direct_value = cline.split(",")[0];
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g14 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String direct_value = (String) line;
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g15_comp_g1 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			String direct_value = cline.split(",")[0];
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class g15 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String direct_value = (String) line;
			return new String(direct_value.substring(1, direct_value.length()));
		}
	}
	
	class m1 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[5]);
		}
	}
	
	class m2 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[6]);
		}
	}
	
	class m3 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[7]);
		}
	}
	
	class m4 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[8]);
		}
	}
	
	class m5 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[9]);
		}
	}
	
	class m6 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[10]);
		}
	}
	
	class m7 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[11]);
		}
	}
	
	class m8 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[12]);
		}
	}
	
	class m9 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[13]);
		}
	}
	
	class m10 implements Function<Object, Object>, Serializable {
		@Override
		public Object apply(Object line) {
			String cline = (String) line;
			return Integer.parseInt(cline.split(",")[14]);
		}
	}
}
