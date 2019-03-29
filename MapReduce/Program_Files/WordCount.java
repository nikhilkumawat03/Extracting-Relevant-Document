package tfidf;

import java.io.IOException;
import java.util.Scanner;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{


      public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
    	  
    //String fileName = ((FileSplit)context.getInputSplit()).getPath().getName().toString();
    	  
		String currentLine[] = line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		/*String words[] = currentLine.split(" ");
		 Pattern p = Pattern.compile("[a-zA-Z]+"); 
	     Matcher m1 = p.matcher(currentLine); 
	     while(m1.find()){
	    	 String word = m1.group();
	    	 word = fileName+"-"+word;
	    	 context.write(new Text(word), new IntWritable(1));
	     }*/
			if(currentLine.length == 10){
				//System.out.println(currentLine[0] + " " + currentLine.length);
				String news[] = currentLine[9].split("\\W");
				String fileName = currentLine[0];
			     for(String word: news){
					word = fileName+"-"+word;
					context.write(new Text(word), new IntWritable(1));	
			     }
			}
      	}
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
    private MultipleOutputs<Text, IntWritable> multipleoutputs;
    public void setup(Context context) throws IOException, InterruptedException{
    	multipleoutputs = new MultipleOutputs<Text, IntWritable>(context);
    }
    
    public void reduce(Text word, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
                       
    double total_no_of_docs = 53291;
	double no_of_docs = 0;
	
	String docs = String.valueOf(total_no_of_docs);
	
    int count = 0;
	Iterator<IntWritable> it = values.iterator();
	IntWritable i;
    while(it.hasNext()){
	i = (IntWritable)it.next();
	count = count + i.get();	
	}
    String pathword = word.toString();
    pathword = pathword+":"+docs;
   // String[] splitted = pathword.split("-");
   // String path = splitted[0];
   // String a_word = splitted[1];
    context.write(new Text(pathword), new IntWritable(count));
    }
   /* public void cleanup(Context context) throws IOException, InterruptedException{
    	multipleoutputs.close();
    }*/
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    
    job.setJarByClass(WordCount.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    Scanner scanner = new Scanner(System.in);
    System.out.println("Enter corpus directory name:");
    String corpus_name = scanner.nextLine();
    System.out.println(corpus_name);
    Path output1 = new Path("/word_count");
    FileSystem  hdfs1 = FileSystem.get(conf);
    if(hdfs1.exists(output1)){
    	hdfs1.delete(output1, true);
    }
    Path output2 = new Path("/wordcount");
    FileSystem  hdfs2 = FileSystem.get(conf);
    if(hdfs2.exists(output2)){
    	hdfs2.delete(output2, true);
    }
    FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9001/"+corpus_name));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9001/wordcount"));
    
     Path inputPath = new Path("hdfs://namenode:9001/"+corpus_name);
	    FileSystem fs = inputPath.getFileSystem(conf);
	    FileStatus[] stat = fs.listStatus(inputPath);
	    job.setJobName(String.valueOf(stat.length));
    
    
    System.out.println("Code is running");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}