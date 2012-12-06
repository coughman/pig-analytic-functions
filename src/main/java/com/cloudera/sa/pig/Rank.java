package com.cloudera.sa.pig;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * rank analytic function.  
 * This assigns same rank for data of same values and it skips ranks if there's more than one identical values before.
 *  
 * @author kaufman
 * @see DenseRank rank function which does not skip
 */
public class Rank extends DenseRank {
	int skip = 0;

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
				rank = rank + skip + 1;
				skip = 0;				
			}
			else {
				skip++;				
			}
		    
			Tuple outputTuple = tupleFactory.newTuple(new Long(rank));
			outputBag.add(outputTuple);						
			
	    }

		return outputBag;
	}

}
