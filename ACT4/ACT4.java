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

public class ACT4 {

public static class MapWritableExtend extends MapWritable{
    
    public String toString(){
      String s = new String("{ ");
      Set<Writable> keys = this.keySet();
      for(Writable key : keys){
         Text count = (Text) this.get(key);
         s =  s + key.toString() + "----------" + count.toString() + "\n";
         //System.out.println("");
      }

      s = s + " }";
      return s;
   }
}

public static class ACT4Mapper extends Mapper<Object, Text, Text, MapWritableExtend> {
   private IntWritable ONE = new IntWritable(1);
   MapWritableExtend map = new MapWritableExtend();
    
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
    		//System.out.println("STRING: " + tokens[1]);
    		//String words[] = tokens[1].trim().split("\\s+");
    		if(words.length > 0) {
    		for (int i = 0; i < words.length; i++) {
              		
              		Text w = new Text(words[i]);  
          		map.clear();
              		for (int j = 0; j < words.length; j++) {
              		if(i != j){
              			Text wordPair = new Text(words[j]);
              			
              			map.put(wordPair,loc);
              				             		
              		}
                   		
              }
              context.write(w, map);
              
          }
         }
      }      
   }// map fn
} // map class


public static class ACT4Reducer extends Reducer<Text,MapWritableExtend,Text,MapWritableExtend> {
   protected static HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
   private IntWritable totalCount = new IntWritable();
   private MapWritableExtend countStripes = new MapWritableExtend();
   
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
  
    protected void reduce(Text key, Iterable<MapWritableExtend> values, Context context) throws IOException, InterruptedException {
	countStripes.clear();
              
        Text c = new Text();
        String k = key.toString();
        HashMap<String, String> lemma = new HashMap<String, String>();

        
        
        k = k.replaceAll("j", "i");
        k = k.replaceAll("v", "u");
        String s = "";
        
        for(MapWritableExtend m:values){
           Set<Writable> keys = m.keySet();
           for (Writable key1 : keys) {
           Text newWordPair = new Text(key1);
            Text lemmaLoc = (Text) m.get(key1);
            if (countStripes.containsKey(newWordPair)) {
                Text t1 = (Text) countStripes.get(newWordPair);
                t1.set(t1 + " " + lemmaLoc);
                countStripes.put(newWordPair,t1);
            } else {
                countStripes.put(newWordPair, lemmaLoc);
            }
          }	       	
       } 
       Text blankKey = new Text();
       blankKey.set(" ");
       context.write(key,countStripes);
       
       /* if(hm.containsKey(k)){
        	ArrayList<String> arr = hm.get(k);
                for(int si=0; si<arr.size();si++){
                	if(!lemma.containsKey(arr.get(si))){
               			lemma.put(arr.get(si),"");
                		
                	}
                }
        }
        else{
               lemma.put(k,"");
                	}  */
        
        
       
        /*for(Writable keys: countStripes.keySet()){
        	Text t1 = (Text)countStripes.get(keys);
        	//String keyNew = keys.toString();
        	Text t2 = new Text(keys.toString());
        	//t2.set(keys);
        	context.write(t2,t1);
        } */
        
    }
}

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(ACT4.class);
    job.setMapperClass(ACT4Mapper.class);
    job.setCombinerClass(ACT4Reducer.class);
    job.setReducerClass(ACT4Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritableExtend.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //FileInputFormat.addInputPath(job, new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    //ACT3Reducer.initLemma(args[1]);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}