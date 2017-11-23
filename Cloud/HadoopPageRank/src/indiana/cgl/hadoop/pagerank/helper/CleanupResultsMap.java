package indiana.cgl.hadoop.pagerank.helper;

/*
 * collect the page rank results from previous computation.
 */

import indiana.cgl.hadoop.pagerank.RankRecord;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanupResultsMap extends Mapper<LongWritable, Text, DoubleWritable, LongWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String strLine = value.toString();
		RankRecord rrd = new RankRecord(strLine);
		//write the rankvalue as key and source URL as value
		//so that the reducer gets the dataset in ascending order(sorted) of the rank values
		context.write(new DoubleWritable(rrd.rankValue), new LongWritable(rrd.sourceUrl));
	}

}
