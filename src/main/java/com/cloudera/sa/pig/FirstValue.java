package com.cloudera.sa.pig;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class FirstValue extends EvalFunc<Object> implements Accumulator<Object> {

	Object first = null;
	

	@Override
	public Object exec(Tuple input) throws IOException {
		
		DataBag inputBag = (DataBag)input.get(0);
	    if(inputBag.size() == 0) {
	    	return null;
	    }
	    for (Tuple t : inputBag) {
	    	Object o = t.get(0);
	    	if (first == null && o != null)
	         	first = o;
	    }
	    
		return first;
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

	@Override
	public void accumulate(Tuple input) throws IOException {
		if (first != null) return;
		
		DataBag inputBag = (DataBag)input.get(0);
		for (Tuple t : inputBag) {
			if (t != null && t.size() > 0 && t.get(0) != null)
			{
				first = t.get(0);
				break;
			}
		}
	}

	@Override
	public Object getValue() {
		return first;
	}

	@Override
	public void cleanup() {
		first = null;		
	}


}
