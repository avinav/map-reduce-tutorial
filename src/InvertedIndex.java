import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class InvertedIndex {
	public static class Posting implements Serializable {
		private static final long serialVersionUID = 1L;
		private String docId;
		private int tf;
		public Posting(String docId, int tf) {
			this.docId = docId;
			this.tf = tf;
		}
		public void setTf(int tf) {
			this.tf = tf;
		}
		public void incrementTf() {
			this.tf ++;
		}
		public int getTf() {
			return this.tf;
		}
		public String getDocId() {
			return docId;
		}
		@Override
		public String toString() {
			return "P(" + docId + "," + tf  + ")";
		}
	}
	
	public static class Map extends MapReduceBase implements
	Mapper<Text, Text, Text, Posting> {
		private Text word = new Text();
		public void map(Text key, Text val, OutputCollector<Text, Posting> output,
				Reporter reporter) throws IOException {
			StringTokenizer tokenizer = new StringTokenizer(val.toString());
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, new Posting(key.toString(), 1));
			}	
		}
	}
	
	public static class Combine extends MapReduceBase implements
	Reducer<Text, Posting, Text, LinkedList<Posting>> {
		private HashMap<String, Posting> docMap = new HashMap<String, Posting>();
		public void reduce(Text key, Iterator<Posting> val, 
				OutputCollector<Text, LinkedList<Posting>> output, Reporter reporter) throws IOException {
			LinkedList<Posting> postingList = new LinkedList<Posting>();
			while(val.hasNext()) {
				Posting newPost = val.next();
				Posting post = docMap.get(newPost.getDocId());
				if( post != null) {
					post.setTf(post.getTf() + newPost.getTf());
				}
				else {
					docMap.put(newPost.getDocId(),newPost);
					postingList.add(newPost);
				}
			}
			output.collect(key, postingList);
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, LinkedList<Posting>, Text, LinkedList<Posting>> {
		public void reduce(Text key, Iterator<LinkedList<Posting>> val,
		OutputCollector<Text, LinkedList<Posting>> output, Reporter reporter) throws IOException{
			LinkedList<Posting> finalList = new LinkedList<Posting>();
			while(val.hasNext()) {
				LinkedList<Posting> newPostList = val.next();
				finalList.addAll(newPostList);
			}
			output.collect(key, finalList);
		}
		
	}
	
	public static void main(String args[]) throws IOException {
	JobConf conf = new JobConf(InvertedIndex.class);
		
		conf.setJobName("InvertedIndex");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LinkedList.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
