package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FbiCases {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		Job job = new Job(conf, "FbiCases");
		job.setJarByClass(FbiCases.class);
		
		job.setMapOutputKeyClass(Text.class);//Represents FBIcode
		job.setMapOutputValueClass(IntWritable.class);//Represents each case
		
		job.setMapperClass(FbiCasesMapper.class);	//Set mapper class
		job.setReducerClass(FbiCasesReducer.class);	//Set reducer class
		

		job.setOutputKeyClass(Text.class);//Represents FBIcodes
		job.setOutputValueClass(IntWritable.class);// Represents number of cases for each FBI code.
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * Input and Output Paths will be provided in command line.
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
	

}
