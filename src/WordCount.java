import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class WordCount {
	
	// <inp_key, inp_value, op_key, op_val>
	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
		@Override
		public void map(LongWritable key, Text val,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = val.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> val,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int ctr = 0;
			while (val.hasNext()) {
				IntWritable count = val.next();
				ctr += count.get();
			}
			output.collect(key, new IntWritable(ctr));
		}
		
	}
}
