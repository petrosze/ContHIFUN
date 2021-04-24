package utils;

import java.util.Map.Entry;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import context.Context;

public class EncoderCreation {

	private Context context;
	
	public EncoderCreation(Context context) {
		this.context = context;
	}
	
	public ExpressionEncoder<Row> createEncoder() {
		StructType structType = new StructType();
		for (Entry<String, java.util.function.Function<Object, Object>> entry : this.context.getFunctions().entrySet()) {
			String func = entry.getKey();
			structType = structType.add(func, context.getTypes().get(func), false);
		}
		return RowEncoder.apply(structType);
	}
}
