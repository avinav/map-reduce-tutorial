import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringBufferInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
	public static class Posting implements Writable {
		
		private String docId;
		private int tf;
		public Posting(){
			
		}
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
			return "(" + docId + " " + tf  + ")";
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			String line = in.readLine();
			String[] val = line.substring(1, line.length()-1).split(" ");
			this.docId = val[0];
			this.tf = Integer.parseInt(val[1]);
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeChars("(" + docId + " " + tf + ")" );
		}
	}
	
	public static class LinkedListWritable<Item extends Writable> extends LinkedList<Item> implements Writable{

		private static final long serialVersionUID = 1L;

		@Override
		public void readFields(DataInput in) throws IOException {
			String line = in.readLine();
			String[] val = line.substring(1,line.length()-1).split(",");
			for (String v : val) {
				Item i = (Item)new Object();
				i.readFields(new DataInputStream(new StringBufferInputStream(v)));
				this.add(i);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeChar('[');
			int ctr = 0;
			for (Item i : this) {
				i.write(out);
				if (ctr < this.size() - 1) {
					out.writeChar(',');
				}
				ctr += 1;
			}
			out.writeChar(']');

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
	Reducer<Text, Posting, Text, LinkedListWritable<Posting>> {
		private HashMap<String, Posting> docMap = new HashMap<String, Posting>();
		public void reduce(Text key, Iterator<Posting> val, 
				OutputCollector<Text, LinkedListWritable<Posting>> output, Reporter reporter) throws IOException {
			LinkedListWritable<Posting> postingList = new LinkedListWritable<Posting>();
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
	Reducer<Text, LinkedListWritable<Posting>, Text, LinkedListWritable<Posting>> {
		public void reduce(Text key, Iterator<LinkedListWritable<Posting>> val,
		OutputCollector<Text, LinkedListWritable<Posting>> output, Reporter reporter) throws IOException{
			LinkedListWritable<Posting> finalList = new LinkedListWritable<Posting>();
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
		conf.setOutputValueClass(LinkedListWritable.class);
		
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
