/**
 * command to run the jar : 
 * hadoop jar StatsCalculation.jar <input_path> <output_path>
 */

package indiana.univ.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class StatsCalculation {

	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
	 	private final static Text randomKey = new Text("A");
	 	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(randomKey,new FloatWritable(Float.parseFloat(value.toString())));	//Send all the values to the same reducer.Use only one key while emitting key value pairs.
		}
	}

	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
			float count = 0;
			float sum = 0;
			float min = 0;
			float max = 0;
			float average = 0;
			float newValue = 0;
			float sumDiffSqr = 0;
			float stdDev = 0;
			ArrayList<Float> list = new ArrayList<Float>();

			for (FloatWritable val : values) {
				count++;	//Increase the count for each iteration.Gives us the total count of the number of values.
				newValue = val.get();
				sum = sum + newValue; //Calculate the sum of all the values
				list.add(newValue); //Add each element to an ArrayList
			}
			
			average = sum / count; //Find the mean
			min = list.get(0);
			max = list.get(0);
			for (Float f : list) {
				if (f < min)
					min = f;
				if (f > max)
					max = f;
				sumDiffSqr += Math.pow(f - average, 2); //Find the sum of the difference of each element from the mean
			}
			stdDev = (float) Math.sqrt(sumDiffSqr / count); //Find the standard deviation of given data set
			
			//Write the calculated statistics to the disk
			context.write(new Text("Min : "), new FloatWritable(min));
			context.write(new Text("Max : "), new FloatWritable(max));
			context.write(new Text("Avg : "), new FloatWritable(average));
			context.write(new Text("StdDev : "), new FloatWritable(stdDev));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: StatsCalculation <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "StatsCalculation");//Name the MapReduce job
		
		//Set the jar name, mapper class, reducer class
		job.setJarByClass(StatsCalculation.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//Set the output key and value types for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		//Set the output key and value types for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		//Set the job input and output paths
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Wait for the job to complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
