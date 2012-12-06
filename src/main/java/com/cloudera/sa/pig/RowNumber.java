package com.cloudera.sa.pig;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class RowNumber extends EvalFunc<DataBag> implements Accumulator<DataBag>{
	
	long rowNumber = 0;
	
	 // output bag for storing tuples of row numbers
    DataBag outputBag = null;
    
	static BagFactory bagFactory = BagFactory.getInstance();
	static TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null) return null;
		
		DataBag inputBag = (DataBag)input.get(0);
	    if(inputBag.size() == 0) {
	    	return null;
	    }
	    // output bag for storing tuples of row numbers
	    DataBag outBag = bagFactory.newDefaultBag();

	    for (int i = 0; i < inputBag.size(); i++) {
	    	Tuple outputTuple = tupleFactory.newTuple(new Long(++rowNumber));
			outBag.add(outputTuple);						
		}

		return outBag;
	}


	@Override
	public void accumulate(Tuple input) throws IOException {
		DataBag inputBag = (DataBag)input.get(0);	    
		outputBag = bagFactory.newDefaultBag();
		
		for (Tuple t : inputBag) {
			if (t != null && t.size() > 0 && t.get(0) != null) {
		    	Tuple outputTuple = tupleFactory.newTuple(new Long(++rowNumber));
		    	outputBag.add(outputTuple);
			}
		}
	}

	@Override
	public DataBag getValue() {
		return outputBag;
	}

	@Override
	public void cleanup() {
		outputBag = null;
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		FieldSchema rowNumberSchema = new Schema.FieldSchema(null, DataType.LONG);
		FieldSchema bagSchema;
		try {
			bagSchema = new Schema.FieldSchema(getSchemaName(this.getClass().getName(), input), 
					new Schema(rowNumberSchema), DataType.BAG);
			return new Schema(bagSchema);			
		} catch (FrontendException e) {
			getLogger().error("could not create bag schema", e);
			return null;
		}
	}

}
