import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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
import java.util.*;

public class Stripes {

public static class MapWritableExtend extends MapWritable{
    
    public String toString(){
      String s = new String("{ ");
      Set<Writable> keys = this.keySet();
      for(Writable key : keys){
         IntWritable count = (IntWritable) this.get(key);
         s =  s + key.toString() + "=" + count.toString() + ",";
      }

      s = s + " }";
      return s;
   }
}
    
public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritableExtend> {
    private IntWritable ONE = new IntWritable(1);
    MapWritableExtend map = new MapWritableExtend();
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
    	String val = value.toString();
    	InputStream ir = new ByteArrayInputStream(val.getBytes());
    	BufferedReader rdr = new BufferedReader(new InputStreamReader(ir));
	for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
		String[] tokens = line.toString().split("\\s+");
			
		if (tokens.length > 1) {
          	for (int i = 0; i < tokens.length; i++) {
          		Text w = new Text(tokens[i]);  
          		map.clear();
              		for (int j = 0; j < tokens.length; j++) {
              		if(i != j){
              			Text wordPair = new Text(tokens[j]);
              			if(map.containsKey(wordPair)){
              				IntWritable counter = (IntWritable)map.get(wordPair);
              				counter.set(counter.get() + 1);
              				map.put(wordPair,counter);
              			}
              			else{
              			map.put(wordPair,new IntWritable(1));
              				
              				
              			}
              		
              		}
                   		
              }
              context.write(w, map);
          }
      }        
   }     
  }
}

public static class IntSumReducer extends Reducer<Text,MapWritableExtend,Text,MapWritableExtend> {
    private MapWritableExtend countStripes = new MapWritableExtend();
    
    protected void reduce(Text key, Iterable<MapWritableExtend> values, Context context) throws IOException, InterruptedException {
       countStripes.clear();
       for(MapWritableExtend m:values){
           Set<Writable> keys = m.keySet();
           for (Writable key1 : keys) {
            IntWritable fromCount = (IntWritable) m.get(key1);
            if (countStripes.containsKey(key1)) {
                IntWritable count = (IntWritable) countStripes.get(key1);
                count.set(count.get() + fromCount.get());
                countStripes.put(key1,count);
            } else {
                countStripes.put(key1, fromCount);
            }
          }	       	
       }
       context.write(key, countStripes);
    }
}

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Stripes.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritableExtend.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  

}