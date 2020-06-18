/***************************************************************
*SortComparator: SecondarySortBasicCompKeySortComparator

*****************************************************************/

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortBasicCompKeySortComparator extends WritableComparator {

  protected SecondarySortBasicCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getZipCode().compareTo(key2.getZipCode());
		if (cmpResult == 0)// same deptNo
		{
			int key1index = (key1.gettaxCityStateHousePair()).indexOf('\t');
			int key2index= (key2.gettaxCityStateHousePair()).indexOf('\t');	
			
			int key1tax = Integer.parseInt(key1.gettaxCityStateHousePair().substring(0,key1index));
			int key2tax = Integer.parseInt(key2.gettaxCityStateHousePair().substring(0,key2index));
			//String newKey1Tax= key1tax+key1.gettaxCityStateHousePair().substring(key1index,key1.gettaxCityStateHousePair().length());
			//String newKey2Tax= key2tax+key2.gettaxCityStateHousePair().substring(key2index,key1.gettaxCityStateHousePair().length());
			
			return 
					-Integer.compare(key1tax,key2tax);
			//If the minus is taken out, the values will be in
			//ascending order
		}
		return cmpResult;
	}
}
