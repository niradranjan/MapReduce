package iu.pti.hbaseapp.clueweb09;

import java.util.PriorityQueue;
import java.util.Stack;

import iu.pti.hbaseapp.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class SearchEngineTester {
	
	public static class PageRecord implements Comparable<PageRecord> {
		String docId;
		String URI;
		float pageRank;
		int termFreq;
		
		public PageRecord(String docId, String URI, float pageRank, int termFreq) {
			this.docId = docId;
			this.URI = URI;
			this.pageRank = pageRank;
			this.termFreq = termFreq;
		}
		
		@Override
		public int compareTo(PageRecord that) {
			if (pageRank < that.pageRank) {
				return -1;
			} else if (pageRank == that.pageRank) {
				return 0;
			} else {
				return 1;
			}
		}
	}
	
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.clueweb09.SearchEngineTester <command> [<parameters>]");
		System.out.println("	Where <command> <parameters> could be one of the following:");
		System.out.println("	search-keyword <keyword>");
		System.out.println("	get-page-snapshot <page document ID>");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			usage();
			System.exit(1);
		}
		
		try {
			String command = args[0];
			if (command.equals("search-keyword")) {
				searchKeyword(args[1]);
			} else if (command.equals("get-page-snapshot")) {
				getPageSnapshot(args[1]);
			} else {
				usage();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void searchKeyword(String keyword) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable dataTable = new HTable(hbaseConfig, Constants.CW09_DATA_TABLE_BYTES);
		HTable indexTable = new HTable(hbaseConfig, Constants.CW09_INDEX_TABLE_BYTES);
		HTable prTable = new HTable(hbaseConfig, Constants.CW09_PAGERANK_TABLE_BYTES);
		
		int topCount = 20;
		// this is the heap for storing the top 20 ranked pages
		PriorityQueue<PageRecord> topPages = new PriorityQueue<PageRecord>(topCount);
		
		// get the inverted index row with the given keyword
        keyword = keyword.toLowerCase();
        byte[] keywordBytes = Bytes.toBytes(keyword);
		Get gIndex = new Get(keywordBytes);
        Result indexRow = indexTable.get(gIndex);
		
        // loop through the document IDs in the row. Recall the schema of the clueWeb09IndexTable:
        // row key: term (keyword), column family: "frequencies", qualifier: document ID, cell value: term frequency in the corresponding document
		int pageCount = 0;
        for (KeyValue kv : indexRow.list()) {
            String pageDocId = null;
            int freq = 0;
            String pageUri = null;
            float pageRank = 0;

			// Write your codes for the main part of implementation here
            // Step 1: get the document ID of one page, as well as the keyword's frequency in that page
            // From the clueWeb09IndexTable, for each word in our list, we try to access all key,value pairs corresponding to it.
            // The key in this case is the Document ID where the word appears.
            // The value denotes the number of times that word occurred in that particular Document.
            pageDocId = Bytes.toString(kv.getQualifier());
            freq = Bytes.toInt(kv.getValue());
			
            // Step 2: get the URI of the page from clueWeb09DataTable
            // Use the previously found Document ID to find out the corresponding URI 
            // from clueWeb09DataTable. 
            byte[] docIdBytes = Bytes.toBytes(pageDocId);
            Get docIdIndex = new Get(docIdBytes);
            Result docIdRow = dataTable.get(docIdIndex);
            pageUri = Bytes.toString(docIdRow.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES));
			
            // Step 3: get the page rank value of this page from clueWeb09PageRankTable
            // Use the previously found Document ID again to find out its corresponding 
            // page rank from clueWeb09PageRankTable
            Result pageRankRow = prTable.get(docIdIndex);
            pageRank = Bytes.toFloat(pageRankRow.list().get(0).getQualifier());


            
		
		    // END of your code

            // Use the heap to select the top 20 pages according to page rank
			PageRecord page = new PageRecord(pageDocId, pageUri, pageRank, freq);
			if (topPages.size() < topCount) {
				topPages.offer(page);
			} else {
				PageRecord head = topPages.peek();
				if (page.pageRank > head.pageRank) {
					topPages.poll();
					topPages.offer(page);
				}
			}
			
			pageCount++;
			if (pageCount % 100 == 0) {
				System.out.println("Evaluated " + pageCount + " pages.");
			}
		}
        System.out.println("Evaluated " + pageCount + " pages.");
		dataTable.close();
		indexTable.close();
		prTable.close();
		
		System.out.println("Evaluated " + pageCount + " pages in total. Here are the top 20 pages according to page ranks:");
		Stack<PageRecord> stack = new Stack<PageRecord>();
		while (topPages.size() > 0) {
			stack.push(topPages.poll());
		}
		while (stack.size() > 0) {
			PageRecord page = stack.pop();
			System.out.println("Document ID: " + page.docId + ", URI: " + page.URI + ", page rank: " + page.pageRank + ", word frequency: "
					+ page.termFreq);
		}
	}
	
	public static void getPageSnapshot(String docId) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable dataTable = new HTable(hbaseConfig, Constants.CW09_DATA_TABLE_BYTES);
		byte[] docIdBytes = Bytes.toBytes(docId);
		Get gDoc = new Get(docIdBytes);
		Result docRow = dataTable.get(gDoc);
		String uri = Bytes.toString(docRow.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES));
		String content = Bytes.toString(docRow.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES));
		System.out.println(uri);
		System.out.println(content);
	}
}
