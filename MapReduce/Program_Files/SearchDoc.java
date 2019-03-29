package tfidf;

import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SearchDoc {
	
	public static class SearchingMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	    private String searchString;
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			System.out.println("Enter to map *****************************************************");
			searchString = context.getConfiguration().get("query");
			System.out.println("Query is:"+searchString);
			String tokens[] = searchString.split(" ");
			String word[] = line.toString().split("\t");
			for(int i=0;i<tokens.length;++i){
				String match[] = word[0].split("-");
				if(match[0].equals(tokens[i])){
					Double tfidf = Double.valueOf(word[1]);
					context.write(new Text(match[1]), new DoubleWritable(tfidf));
				}
			}	
		}
	}	
	
	public static class SumReducer 
	extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

	    public void reduce(Text key, Iterable<DoubleWritable> tfidf, Context context) throws IOException, InterruptedException {
		double sum = 0;
		Iterator it = tfidf.iterator();
	        while(it.hasNext()){
			DoubleWritable i = (DoubleWritable)it.next();
			sum = sum + i.get();	
		}
		context.write(new Text(key), new DoubleWritable(sum));
	    }
	  }
	
	
	public static void main(String[] args) throws Exception {
		
		//Setting up configuration
	    Configuration conf = new Configuration();
	    
	    System.out.println("Enter Searching string:");
	    Scanner scanner = new Scanner(System.in);
	    String query = scanner.nextLine();
	    conf.set("query", query);
	    
	    Job job = Job.getInstance(conf, "Doc search");
	    
	    //Setting up jar class
	    job.setJarByClass(SearchDoc.class);
	    
	    //Setting up mapper reducer class
	    job.setMapperClass(SearchingMapper.class);
	    job.setCombinerClass(SumReducer.class);
	    job.setReducerClass(SumReducer.class);
	    
	    //Setting up output class
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    
	    //Taking query input
	    FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9001/tfidf"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9001/search_res"));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
