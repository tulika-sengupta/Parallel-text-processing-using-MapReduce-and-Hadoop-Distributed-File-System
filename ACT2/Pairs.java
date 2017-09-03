import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.WordPair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import WordPair.WordPair;
import java.io.StringReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

public class Pairs {

public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
    	String val = value.toString();
    	InputStream ir = new ByteArrayInputStream(val.getBytes());
    	BufferedReader rdr = new BufferedReader(new InputStreamReader(ir));
	for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
		String[] tokens = line.toString().split("\\s+");	
		if (tokens.length > 1) {
          	for (int i = 0; i < tokens.length; i++) {
              		for (int j = i + 1; j < tokens.length; j++) {
              			Text wordPair = new Text();
                   		wordPair.set(tokens[i] + " " + tokens[j]);
                   		context.write(wordPair, ONE);
              }
          }
      }        
}     
   
}
}

public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable totalCount = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Pairs.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  

}