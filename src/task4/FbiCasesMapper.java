package task4;
/*
 * This dataset contains attributes related to crimes taking place in various areas like type of
 * crime, FBI code related to that criminal case, arrest frequency, location of crime etc.
 * 
 * Write a MapReduce/Pig program to calculate the number of arrests done between October 2014 and
 * October 2015.
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;  
import java.util.Date;

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
		 * 
		 * It will also skip records that don't have the right date format (MM/dd/yyyy)
		 */
		try {

			/*
			 * Arrest Column is represented by index 8.
			 * 
			 * Date Column is represented by index 2.
			 * Actual date is a substring(0,10)
			 */
			
			String [] line = value.toString().split(",");
			
			/*
			 * If index 8 contains "true" and the date falls between the limits
			 * then arrest will have 1 count toward summation
			 */
			if(line[8].equalsIgnoreCase("true")  &&  compareDate(line[2].substring(0, 10)))
					context.write(new Text("Arrests"), new IntWritable(1)); //($District,1)
				
			
		}
		catch(ArrayIndexOutOfBoundsException e) {//Skip this line if not enough columns
		}
		catch(Exception e) {//Skip this line if date is not proper format (MM/dd/yyyy)
		}
	}
	
	
	/*
	 * This method will abstract the date comparison.
	 * If the parameter falls within the limits it returns true
	 */
	private static boolean compareDate (String a) throws Exception {
		/*
		 * Since the time-frame requirement is between 10/2014 and 10/2015,
		 * both months must be excluded from consideration which means
		 * the earliest boundary is 10/31/2014
		 * and the latest boundary is 10/01/2015  
		 */
		
		Date earliest=new SimpleDateFormat("MM/dd/yyyy").parse("10/31/2014"); 	//Converting boundary to Date
		Date latest=new SimpleDateFormat("MM/dd/yyyy").parse("10/01/2015");		//Converting boundary to Date
		Date dateA=new SimpleDateFormat("MM/dd/yyyy").parse(a);					//Converting parameter 'a' to Date
		
		if(earliest.before(dateA) && latest.after(dateA))
			return true;
		else
			return false;
	}

}
