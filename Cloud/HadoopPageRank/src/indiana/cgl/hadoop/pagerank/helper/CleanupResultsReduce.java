package indiana.cgl.hadoop.pagerank.helper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanupResultsReduce extends Reducer<DoubleWritable, LongWritable, LongWritable, Text>{
	List<String> list = new ArrayList<String>();

	public void reduce(DoubleWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		//reducer gets dataset in ascending order of rankvalue.rank value is key and source URL is value here.
		//We need the dataset in descending order of rankvalue.
		//So, we store the records in the format <SourceURL#RankValue> in an array list
		for (LongWritable value: values){
			list.add(value.toString()+"#"+key.toString());
		}

	}
	//the cleanup method is called only once after the execution of all the reducer tasks.
	//Here we simply take the list of <SourceURL#RankValue> strings and print the last 10 strings
	//As we already have the records in ascending order of rank value, the last 10 values give us
	//our top 10 records
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		ListIterator<String> li = list.listIterator(list.size());
		int counter = 0;
		while(li.hasPrevious()) {
			if (counter >= 10) {
				break;
			}
			String[] arr = li.previous().split("#");
			context.write(new LongWritable(Long.parseLong(arr[0])), new Text(arr[1]) );
			counter++;

		}

	}

}
