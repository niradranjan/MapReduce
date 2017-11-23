package indiana.cgl.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;

public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

	// each map task handles one line within an adjacency matrix file
	// key: file offset
	// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int numUrls = context.getConfiguration().getInt("numUrls",1);
		String line = value.toString();
		StringBuffer sb = new StringBuffer();
		// instance an object that records the information for one webpage
		RankRecord rrd = new RankRecord(line);
		double calc = 0;
		// double rankValueOfSrcUrl;
		if (rrd.targetUrlsList.size()<=0){
			// there is no out degree for this webpage; 
			// scatter its rank value to all other urls
			double rankValuePerUrl = rrd.rankValue/(double)numUrls;
			for (int i = 0 ; i < numUrls ; i++){
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
			}
		} else {
			/*Write your code here*/
			//calculate the rank value of URLs that have a target list
			calc = rrd.rankValue/(double)rrd.targetUrlsList.size();

			//write each target URL and corresponding rank value
			for (int i = 0 ; i < rrd.targetUrlsList.size() ; i++) {
				sb.append("#" + rrd.targetUrlsList.get(i));
				context.write(new LongWritable(rrd.targetUrlsList.get(i)), new Text(String.valueOf(calc)));
			}
		} //for
		//if there is no outgoing link from a URL, write the URL only
		context.write(new LongWritable(rrd.sourceUrl), new Text(sb.toString()));
	} // end map

}
