package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class WordCountClueWeb09 {
	
	static class WcMapper extends TableMapper<Text, LongWritable> {
		@Override
		public void map(ImmutableBytesWritable row, Result result,	Context context) throws IOException, InterruptedException {
			byte[] contentBytes = result.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			
			
			//Pass the content to the getWordFreq() method.The method will return a HashMap
			//which contains a list of key-value pairs.The keys are all the unique words in the content.
			//The values are the corresponding count of each unique word.
			//Write each word and its count to the context and pass it to the reducer.
			
			HashMap<String, Long> wordFrequencyMap = getWordFreq(content);

				for(String word: wordFrequencyMap.keySet()) {
					context.write(new Text(word), new LongWritable(wordFrequencyMap.get(word)));
				}

		}
	}

    public static class WcReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
    	@Override
        public void reduce(Text word, Iterable<LongWritable> freqs, Context context)
                throws IOException, InterruptedException {
            
            //In each run of the reducer, we have a word and its corresponding wordcount from
            //individual mappers.We simply add the counts of each word and write this data into 
            //the Hbase table.
            //We create a put variable, where each word is the row key and the corresponding value is 
            //the row value.ColumnFamily is frequencies and ColumnName is count.
            //We finally add the put variable into the HBase table.
            //Reducer ouput is of the type <ImmutableBytesWritable,Writable>
    		long totalFreq = 0;
    		
    		for(LongWritable val : freqs)
			{
				totalFreq += val.get();
			}
			
			Put columnFamilyData = new Put(Bytes.toBytes(word.toString()));
			columnFamilyData.add(Bytes.toBytes(Constants.CF_FREQUENCIES), Bytes.toBytes(Constants.QUALIFIER_COUNT), Bytes.toBytes(totalFreq));
			
			//We have already added the required row into HBase table
			//Reducer output is of the type <ImmutableBytesWritable,Writable>, write to the context
			context.write(new ImmutableBytesWritable(Bytes.toBytes(word.toString())) , columnFamilyData);




			
        }
    }
    
    /**
	 * Tokenize the given "text" with a Lucene analyzer, count the frequencies of all the words in "text", and
	 * return a map from the words to their frequencies.
	 * @param text A string to be tokenized.
	 * @return 
	 */
	public static HashMap<String, Long> getWordFreq(String text) {
		HashMap<String, Long> freqs = new HashMap<String, Long>();
		try {
			Analyzer analyzer = Constants.analyzer;			
			TokenStream ts = analyzer.reusableTokenStream("dummyField", new StringReader(text));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (Helpers.isNumberString(termVal)) {
					continue;
				}
				
				if (freqs.containsKey(termVal)) {
					freqs.put(termVal, freqs.get(termVal)+1);
				} else {
					freqs.put(termVal, 1L);
				}
			}
			ts.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return freqs;
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
	    Scan scan = new Scan();
	    scan.addColumn(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
		Job job = new Job(conf,	"Counting words from " + Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(WordCountClueWeb09.class);	
		TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_DATA_TABLE_NAME, scan, WcMapper.class, Text.class, LongWritable.class, job, true);
		TableMapReduceUtil.initTableReducerJob(Constants.WORD_COUNT_TABLE_NAME, WcReducer.class, job);	
		job.setNumReduceTasks(4);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}