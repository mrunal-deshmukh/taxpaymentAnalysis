/***************************************************************
*CustomWritable for the composite key: CompositeKeyWritable
****************************************************************/

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKeyWritable implements Writable,
  	WritableComparable<CompositeKeyWritable> {

	private String zipcode;
	private String taxCityStateHousePair;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String zipcode, String taxCityStateHousePair) {
		this.zipcode = zipcode;
		this.taxCityStateHousePair = taxCityStateHousePair;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(zipcode).append("\t")
				.append(taxCityStateHousePair)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		zipcode = WritableUtils.readString(dataInput);
		taxCityStateHousePair = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, zipcode);
		WritableUtils.writeString(dataOutput, taxCityStateHousePair);
	}

	public int compareTo(CompositeKeyWritable objKeyPair) {
		// TODO:
		/*
		 * Note: This code will work as it stands; but when CompositeKeyWritable
		 * is used as key in a map-reduce program, it is de-serialized into an
		 * object for comapareTo() method to be invoked;
		 * 
		 * To do: To optimize for speed, implement a raw comparator - will
		 * support comparison of serialized representations
		 */
		int result = zipcode.compareTo(objKeyPair.zipcode);
		if (0 == result) {
			int index = taxCityStateHousePair.indexOf('\t');
			int objIndex= objKeyPair.taxCityStateHousePair.indexOf('\t');	
			
			int tax = Integer.parseInt(taxCityStateHousePair.substring(0,index));
			int objTax = Integer.parseInt(objKeyPair.taxCityStateHousePair.substring(0,objIndex));
			//String newTax= tax+taxCityStateHousePair.substring(index,taxCityStateHousePair.length());
			//String newTaxObj= objTax+taxCityStateHousePair.substring(index,taxCityStateHousePair.length());
			result = Integer.compare(tax,objTax);
		}
		return result;
	}

	public String getZipCode() {
		return zipcode;
	}

	public void setZipCode(String zipcode) {
		this.zipcode = zipcode;
	}

	public String gettaxCityStateHousePair() {
		return taxCityStateHousePair;
	}

	public void settaxCityStateHousePair(String taxCityStateHousePair) {
		this.taxCityStateHousePair = taxCityStateHousePair;
	}
}
