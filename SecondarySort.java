package com.neu.temparature;
import java.io.DataInput;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.neu.temparature.NoCombiner.TemparatureReducer;
import com.neu.temparature.NoCombiner.TempparatureMapper;

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class SecondarySort {

// Creating a customwritable class containing stationId and year values
// created a compareTo method to compare two customwritable types and included getters and setters.
public static class CompositeKeyWritable implements Writable,
  	WritableComparable<CompositeKeyWritable> {

	private String stationId;
	private String Year;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String stationId, String Year) {
		this.stationId = stationId;
		this.Year = Year;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(stationId).append("\t")
				.append(Year)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		stationId = WritableUtils.readString(dataInput);
		Year = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, stationId);
		WritableUtils.writeString(dataOutput, Year);
	}

	public int compareTo(CompositeKeyWritable objKeyPair) {
		int result = stationId.compareTo(objKeyPair.stationId);
		if (0 == result) {
			result = Year.compareTo(objKeyPair.Year);
		}
		return result;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public String getYear() {
		return Year;
	}

	public void setYear(String Year) {
		this.Year = Year;
	}
}

public static class SecondarySortBasicMapper extends
	Mapper<Object, Text, CompositeKeyWritable, Text> {
	
	// In map each line is split at comma and compositevalue with (stationId and year) is set as key
	// if the value is either tmax/tmin then map emits the compositevalue as key and string containing
	// year,Tmax/Tmin and corresponding temperature.
@Override
public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {

	String[] inputs = value.toString().split(",");
	String year_value = inputs[1].substring(0, 4);
	if (inputs[2].equals("TMAX") || inputs[2].equals("TMIN")){
		String mapper_value = year_value+","+inputs[2] + "," + inputs[3];
		context.write(
				new CompositeKeyWritable(
						inputs[0].toString(),
						year_value.toString()),new Text(mapper_value));
	}
	}

}

public static class SecondarySortBasicPartitioner extends
	Partitioner<CompositeKeyWritable, Text> {

	// To see that all the same stationIds go to same reducer we have custom partitioner.
	// Integer.MaxValue is to make sure that partitioner doesn't go to a negative value.
@Override
public int getPartition(CompositeKeyWritable key, Text value,
		int numPartitions) {

	return ((key.getStationId().hashCode() & Integer.MAX_VALUE) % numPartitions);
}
}


public static class SecondarySortBasicCompKeySortComparator extends WritableComparator {
	// Keysort comparator compares the stationId first and if the stationId's are equal
	// then it sorts the year according to either ascending order or descending order.
  protected SecondarySortBasicCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getStationId().compareTo(key2.getStationId());
		if (cmpResult == 0)
		{
			return key1.getYear()
					.compareTo(key2.getYear());
		}
		return cmpResult;
	}
}

public static class SecondarySortBasicGroupingComparator extends WritableComparator {
	// In grouping compartor station Id's are compared to other to sort and it doesn't
	// year in here.
	  protected SecondarySortBasicGroupingComparator() {
			super(CompositeKeyWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
			CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
			return key1.getStationId().compareTo(key2.getStationId());
		}
	}

public static class SecondarySortBasicReducer
	extends
	Reducer<CompositeKeyWritable, Text, NullWritable, Text> {
	// Inorder to get the required output we create a temp string and add stationId to it.
	// all the values are iterated and then tmax,tmin,tmax_count and tmin_count are calculated.
	// when the year is changed we add these calculated values, find the mean_tmin and mean_tmax and 
	// concatenated to string.
@Override
public void reduce(CompositeKeyWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {

	String intt = key.getStationId()+","+"[";
	double tmax = 0.0;
	double tmin = 0.0;
	double tmin_count = 0;
	double tmax_count = 0;
	double tmax_avg = 0;
	double tmin_avg = 0;		
	int year_prev = 0;
	int current_year = 0;
	for (Text value : values) {
		String[] temps = value.toString().split(",");
		 
		current_year = Integer.parseInt(temps[0]);
		
		if (year_prev != current_year){
			if (year_prev != 0){
				// If the tmin_count is 0 and dividing by causes exception, we add tmin_avg to be -9999
				// The same is applicable for tmax_count too.
				if(tmin_count == 0){
					tmin_avg = -9999;
					tmax_avg = tmax / tmax_count;
				}				
				else if(tmax_count == 0){
					tmax_avg = -9999;
					tmin_avg = tmin / tmin_count;
				}else{
					tmax_avg = tmax / tmax_count;
					tmin_avg = tmin / tmin_count;
				}
				intt += "("+year_prev+","+tmin_avg+","+tmax_avg+")";
			}
			tmin = 0;
			tmin_count = 0;
			tmax_count = 0;
			tmax = 0;
		}
		
		try {
			if (temps[1].equals("TMAX")) {
				tmax += Double.parseDouble(temps[2]);
				tmax_count++;
			} else if (temps[1].equals("TMIN")) {
				tmin += Double.parseDouble(temps[2]);
				tmin_count++;
			}
		} catch (NumberFormatException e) {
			
		}
					
		year_prev = current_year;
	}
	if(tmin_count == 0){
		tmin_avg = -9999;
		tmax_avg = tmax / tmax_count;
	}				
	else if(tmax_count == 0){
		tmax_avg = -9999;
		tmin_avg = tmin / tmin_count;
	}else{
		tmax_avg = tmax / tmax_count;
		tmin_avg = tmin / tmin_count;
	}
	intt += "("+year_prev+","+tmin_avg+","+tmax_avg+")";
	intt += "]";
	// Since the output format in the requirements doesn't have space between key and value
	// the key is set to Null and stationId is add in the value string.
	context.write(NullWritable.get(), new Text(intt));

}
}


}
