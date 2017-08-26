package task1;
/*
 * This dataset contains attributes related to crimes taking place in various areas like type of
 * crime, FBI code related to that criminal case, arrest frequency, location of crime etc.
 * 
 * Write a MapReduce/Pig program to calculate the number of cases investigated under each FBI
 * code
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
			 * FBICode is in column 15 which corresponds to index 14 in this String array
			 * Every case will be represented with 1 count.
			 */
			
			String fbiCode = value.toString().split(",")[14];
			
			context.write(new Text(fbiCode), new IntWritable(1));
			} catch(ArrayIndexOutOfBoundsException e) {
				//Skip this line
		}
	}

}
