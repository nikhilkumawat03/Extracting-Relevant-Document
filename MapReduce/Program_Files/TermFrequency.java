package tfidf;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TermFrequency {
	
	public static class TermMapper 
		extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			
		//	String fileName = ((FileSplit)context.getInputSplit()).getPath().getName().toString();
			String[] contents = line.toString().split("-");
			String fileName = contents[0];
			String value = contents[1];
			//System.out.println(fileName + "***" + value + "***********");
			
			context.write(new Text(fileName), new Text(value));
		}
	}
	
	public static class TermReducer extends Reducer<Text, Text, Text, Text>{
		
		private MultipleOutputs<Text, Text> multipleoutputs;
		
		public void setup(Context context) throws IOException, InterruptedException{
	    	multipleoutputs = new MultipleOutputs<Text, Text>(context);
	    }
		
		public void reduce(Text fileName, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			double sumOfTermInDocument = 0;
			HashMap<String, Double> map = new HashMap<String, Double>();
			
			for(Text val : value){
				//System.out.println(val + "***" + fileName);
				String wordCount[] = val.toString().split("\t");	//word	frequency	OR 	(the	7)		("Actual String" the	7)
				map.put(wordCount[0], Double.parseDouble(wordCount[1]));	//wordCount[0] = the wordCount[1] = 7
				//Below statement count total number of words in file
				sumOfTermInDocument += Integer.parseInt(wordCount[1]);	//wordCount[1] = 7
			}
			
			for(String wordKey : map.keySet()){	//wordKey: the
				double result = (map.get(wordKey))/sumOfTermInDocument;
				String s = String.format("%.6f", result);
				wordKey = fileName+"-"+wordKey;
				context.write(new Text(wordKey), new Text(s));
			}
		}
		
	    /*public void cleanup(Context context) throws IOException, InterruptedException{
	    	multipleoutputs.close();
	    }*/
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Term Frequency");
	    
	    job.setJarByClass(TermFrequency.class);
	    System.out.println("Class is set");
	    
	    job.setMapperClass(TermMapper.class);
	    job.setCombinerClass(TermReducer.class);
	    job.setReducerClass(TermReducer.class);
	    System.out.println("Mapper reducer class set");
	    

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    System.out.println("Reducer keyvalue class set");
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    System.out.println("Map keyvalue class set");
	    
	   //LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    Path output1 = new Path("/TermFrequency_1");
	    FileSystem  hdfs1 = FileSystem.get(conf);
	    if(hdfs1.exists(output1)){
	    	hdfs1.delete(output1, true);
	    }
	    Path output2 = new Path("/TermFrequency");
	    FileSystem  hdfs2 = FileSystem.get(conf);
	    if(hdfs2.exists(output2)){
	    	hdfs2.delete(output2, true);
	    }
	    FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9001/wordcount"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9001/TermFrequency"));
	    System.out.println("File input/output path set");
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}