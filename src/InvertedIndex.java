import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;


public class InvertedIndex {
	private static final Pattern pattern = Pattern.compile("\\d+");
	
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
//			String line = in.readLine();
//			String[] val = line.substring(1, line.length()-1).split(" ");
//			this.docId = val[0];
//			Matcher matcher = pattern.matcher(val[1]);
//			if (matcher.find()) 
//			this.tf = Integer.parseInt(matcher.group(0));
			Text str = new Text();
			str.readFields(in);
			docId = str.toString();
			IntWritable i = new IntWritable();
			i.readFields(in);
			tf = i.get();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			new Text(docId).write(out);
			new IntWritable(tf).write(out);
		}
	}
	
	public static class LinkedListWritable<Item extends Writable> extends LinkedList<Posting> implements Writable{

		private static final long serialVersionUID = 1L;

		@Override
		public void readFields(DataInput in) throws IOException {
//			String line = in.readLine();
//			String[] val = line.substring(1,line.length()-1).split(",");
//			1st way
//			for (String v : val) {
//				Item i = (Item) new Object();
//				i.readFields(new DataInputStream(new ByteArrayInputStream(v.getBytes())));
//				this.add(i);
//			}
//			2nd way
//			for (String v: val) {
//				Posting p = (Posting) new Object();
//				String[] pval = v.substring(1, v.length()-1).split(" ");
//				p.docId = val[0];
//				Matcher matcher = pattern.matcher(val[1]);
//				if (matcher.find()) 
//				p.tf = Integer.parseInt(matcher.group(0));
//				this.add(p);
//			}
			// 3rd way
			int s = in.readInt();
			for (int i = 0; i < s; i++) {
				Text docId = new Text();
				docId.readFields(in);
				IntWritable tf = new IntWritable();
				tf.readFields(in);
				Posting post = new Posting(docId.toString(), tf.get());
				this.add(post);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// 1st way
//			out.writeChars("[");
//			int ctr = 0;
//			for (Item i : this) {
//				i.write(out);
//				if (ctr < this.size() - 1) {
//					out.writeChar(',');
//				}
//				ctr += 1;
//			}
//			out.writeChars("]");
			 
			// 2nd way
//			int ctr = 0;
//			StringBuilder sb = new StringBuilder();
//			sb.append("[");
//			for (Posting post : this) {
////				Posting post = (Posting) i;
//				sb.append("(" + post.docId);
//				sb.append(" " + post.tf + ")");
//				
//				if (ctr < this.size() - 1) {
//					sb.append(",");
//				}
//				ctr += 1;
//			}
//			sb.append("]");
//			out.writeChars(sb.toString());
			//3rd way
			new IntWritable(this.size()).write(out);
			for (Posting post: this) {
				post.write(out);
			}
		}
		
		@Override
		public String toString() {
			int ctr = 0;
			StringBuilder sb = new StringBuilder();
			sb.append("[");
			for (Posting post : this) {
				sb.append(post.toString());
				if (ctr < this.size() - 1) {
					sb.append(",");
				}
				ctr += 1;
			}
			sb.append("]");
			return sb.toString();
		}
		
	}
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, Posting> {
		private Text word = new Text();
		
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(val.toString());
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, new Posting(getFileName(context), 1));
			}	
		}
		public String getFileName(Context context) {
			FileSplit fs = (FileSplit) context.getInputSplit();
		    return fs.getPath().getName();
		}
	}
	
	public static class Combine extends Reducer<Text, Posting, Text, LinkedListWritable<Posting>> {
		private HashMap<String, Posting> docMap = new HashMap<String, Posting>();
		
		public void reduce(Text key, Iterator<Posting> val, Context context) throws IOException, InterruptedException {
			LinkedListWritable<Posting> postingList = new LinkedListWritable<Posting>();
			while(val.hasNext()) {
				Posting newPost = val.next();
				/*Posting post = docMap.get(newPost.getDocId());
				if( post != null) {
					post.setTf(post.getTf() + newPost.getTf());
				}
				else {
					docMap.put(newPost.getDocId(),newPost);
					postingList.add(newPost);
				}*/
				postingList.add(newPost);
			}
			context.write(key, postingList);
		}
	}

	public static class Reduce extends Reducer<Text, LinkedListWritable<Posting>, Text, LinkedListWritable<Posting>> {
		
		public void reduce(Text key, Iterator<LinkedListWritable<Posting>> val,
				Context context) throws IOException, InterruptedException{
			LinkedListWritable<Posting> finalList = new LinkedListWritable<Posting>();
			while(val.hasNext()) {
				LinkedListWritable<Posting> newPostList = val.next();
				finalList.addAll(newPostList);
			}
			context.write(key, finalList);
		}
		
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Posting.class);	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LinkedListWritable.class);
		
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Combine.class);
		job.setReducerClass(Combine.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(InvertedIndex.class);
		job.waitForCompletion(true);
	}
}



	
