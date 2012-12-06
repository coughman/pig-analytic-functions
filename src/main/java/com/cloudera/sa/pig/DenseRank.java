package com.cloudera.sa.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * dense rank analytic function.  
 * This assigns same rank for data of same values.  For value immediately after this function will assign a rank value increment by 1.
 *  
 * @author kaufman
 * @see Rank rank function that skips if there are more than one identical values
 */
public class DenseRank extends EvalFunc<DataBag> {
	static BagFactory bagFactory = BagFactory.getInstance();
	static TupleFactory tupleFactory = TupleFactory.getInstance();	

	long rank = 0;
	Object lastObject = null;

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null) return null;
		
		DataBag inputBag = (DataBag)input.get(0);
	    if(inputBag.size() == 0) {
	    	return null;
	    }
	    
	    // output bag for storing tuples of results
	    DataBag outputBag = bagFactory.newDefaultBag();
	    
	    for (Tuple t : inputBag) {
	    	Object o = t.get(0);
	    	
			if (!o.equals(lastObject))
			{
				lastObject = o;
				++rank;
			}
		    
			Tuple outputTuple = tupleFactory.newTuple(new Long(rank));
			outputBag.add(outputTuple);						
			
	    }

		return outputBag;
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
