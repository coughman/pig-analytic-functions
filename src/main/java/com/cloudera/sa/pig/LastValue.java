package com.cloudera.sa.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class LastValue extends EvalFunc<Object> {

	Object last = null;


	@Override
	public Object exec(Tuple input) throws IOException {
		DataBag inputBag = (DataBag)input.get(0);
	    if(inputBag.size() == 0) {
	    	return null;
	    }
	    
	    for (Tuple t : inputBag) {
	    	Object o = t.get(0);
	    	if (t != null && o != null)
	         	last = o;
	    }
	    
		return last;
	}
	

	@Override
	public Schema outputSchema(Schema input) {
		try {
			return new Schema(new Schema.FieldSchema(input.getField(0)));
		} catch (FrontendException e) {
			getLogger().error("could not create field schema", e);
			return null;
		}
	}
	
}
