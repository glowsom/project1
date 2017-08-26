package task3;
/*
 * This dataset contains attributes related to crimes taking place in various areas like type of
 * crime, FBI code related to that criminal case, arrest frequency, location of crime etc.
 * 
 * Write a MapReduce/Pig program to calculate the number of arrests in theft district wise.
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Dataset Description:
 * ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description, Arrest,
 * Domestic,Beat,District,Ward,Community Area,FBICode,X Coordinate,Y Coordinate,Year,
 * Updated On,Latitude,Longitude,Location
 */

public class FbiCasesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {		
		
		/*
		 * Some records are be invalid because they don't seem to have enough columns
		 * and are therefore throwing an ArrayIndexOutOfBoundsException.
		 * This try -catch block will skip those records
		 */
		try {

			/*
			 * The Primary Type Column must be checked for "THEFT". It's the 6th column which
			 * corresponds to index 5 in this String Array (line).
			 * 
			 * Arrest Column is represented by index 8
			 * 
			 * Districts are contained in column 12 which corresponds to index 11 in the String Array
			 */
			
			String [] line = value.toString().split(",");
			
			/*
			 * If index 5 contains "THEFT" & index 8 contains "true" 
			 * then the District will have 1 count toward aggregation
			 */
			if(line[5].equalsIgnoreCase("THEFT") && line[8].equalsIgnoreCase("true"))
				context.write(new Text(line[11]), new IntWritable(1)); //($District,1)
			
			} catch(ArrayIndexOutOfBoundsException e) {
				//Skip this line
		}
	}

}
