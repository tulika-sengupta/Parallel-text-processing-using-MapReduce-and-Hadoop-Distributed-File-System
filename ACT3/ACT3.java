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
import java.io.File;
import java.util.*;

public class ACT3 {

public static class ACT3Mapper extends Mapper<Object, Text, Text, Text> {
    private IntWritable ONE = new IntWritable(1);
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String val = value.toString();
    	InputStream ir = new ByteArrayInputStream(val.getBytes());
    	BufferedReader rdr = new BufferedReader(new InputStreamReader(ir));
    	for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
    		String[] tokens = line.toString().split(">");
    		Text loc = new Text();
    		loc.set(tokens[0]+">");    //Retrieving location. 
    		
    		tokens[1] = tokens[1].replaceAll("[^A-Za-z0-9 ]", "");
    		String words[] = tokens[1].trim().toLowerCase().split("\\s+");
    	
    		//String words[] = tokens[1].trim().split("\\s+");
    		if(words.length > 0) {for (int i = 0; i < words.length; i++) {
              		
              			Text wordPair = new Text(words[i]);
                   		//wordPair.set(words[i]);
                   		context.write(wordPair, loc);
              
          }
          }
      }      
  
    	
    		/*for(String key1: hm.keySet()){
		Text wordPair1 = new Text();
		Text wordPair2 = new Text();
		wordPair2.set(" ");
                wordPair1.set(key1);
                ArrayList<String> arr = hm.get(key1);
                for(int s=0; s<arr.size();s++){
                	wordPair2.set(wordPair2.toString() + arr.get(s) + " ");
                } 
                //wordPair2.set(hm.get(key1));
                context.write(wordPair1, wordPair2);
	}*/
    
   }// map fn
} // map class


public static class ACT3Reducer extends Reducer<Text,Text,Text,Text> {
   protected static HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();

   
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	File file = new File("demo_lemma.csv");
    	
    	Scanner inputStream = new Scanner(file);
        String line = null;
        
        while(inputStream.hasNext()){
            line = inputStream.nextLine();
            String[] splitLine = line.split(",");
            ArrayList<String> tempStr = new ArrayList<String>();
            for(int i = 1; i< splitLine.length; i++){
            	tempStr.add(splitLine[i]);
            }
            
            hm.put(splitLine[0],tempStr);          
        }   
    } 
    
    protected static void initLemma(String lemmaName) throws IOException, InterruptedException{
    	File file = new File(lemmaName);
    	
    	Scanner inputStream = new Scanner(file);
        String line = null;
        
        while(inputStream.hasNext()){
            line = inputStream.nextLine();
            String[] splitLine = line.split(",");
            ArrayList<String> tempStr = new ArrayList<String>();
            for(int i = 1; i< splitLine.length; i++){
            	tempStr.add(splitLine[i]);
            }
            
            hm.put(splitLine[0],tempStr);          
        }   
    }


    private IntWritable totalCount = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
       /*for(String key1: hm.keySet()){
		Text wordPair1 = new Text();
		Text wordPair2 = new Text();
		wordPair2.set(" ");
                wordPair1.set(key1);
                ArrayList<String> arr = hm.get(key1);
                for(int s=0; s<arr.size();s++){
                	wordPair2.set(wordPair2.toString() + arr.get(s) + " ");
                } 
                //wordPair2.set(hm.get(key1));
                context.write(wordPair1, wordPair2);
	}*/
       
        //List<Text> cache = new ArrayList<Text>();
        Text c = new Text();
        String k = key.toString();
        HashMap<String, String> lemma = new HashMap<String, String>();

        
        
        k = k.replaceAll("j", "i");
        k = k.replaceAll("v", "u");
        String s = "";
        
        
        if(hm.containsKey(k)){
        	ArrayList<String> arr = hm.get(k);
                for(int si=0; si<arr.size();si++){
                	if(!lemma.containsKey(arr.get(si))){
               			lemma.put(arr.get(si),"");
                		
                	}
                }
        }
        else{
               lemma.put(k,"");
                	}
        
        
        String str="";
        
        for(Text t: values){
        	str = str + " " + t;    	
        }
        
        
        for(String keys: lemma.keySet()){
        		lemma.put(keys,str);
        	}
        
        
        for(String keys: lemma.keySet()){
        	Text t1 = new Text(keys);
        	Text t2 = new Text(lemma.get(keys));
        	context.write(t1,t2);
        }
        
    }
}

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(ACT3.class);
    job.setMapperClass(ACT3Mapper.class);
    job.setCombinerClass(ACT3Reducer.class);
    job.setReducerClass(ACT3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //FileInputFormat.addInputPath(job, new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    //ACT3Reducer.initLemma(args[1]);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  

}