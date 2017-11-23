package indiana.cgl.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReduce extends Reducer<LongWritable, Text, LongWritable, Text>{
	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		double sumOfRankValues = 0.0;
		//String targetUrlsList = "";
		StringBuilder urlList = new StringBuilder();

		int count = 0;

		//int sourceUrl = (int)key.get();
		int numUrls = context.getConfiguration().getInt("numUrls",1);

		//hints each tuple may include: rank value tuple or link relation tuple  
		for (Text value: values){
			String [] arr = value.toString().split("#");
			/*Write your code here*/
			if(arr[0].isEmpty()) {
				count = 1;
				for(int i = 1; i < arr.length; i++) {
					//targetUrlsList.concat("#" + strArray[i]);
					urlList.append("#" + arr[i]);
				}
			}
			else {
				sumOfRankValues += Double.parseDouble(arr[0]);
			}
		} // end for loop
		if(count == 0)
			urlList.append("#");


		sumOfRankValues = 0.85*sumOfRankValues+0.15*(1.0)/(double)numUrls;
		context.write(key, new Text(sumOfRankValues+urlList.toString()));
	}
}