package tfidf;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class InverseDocFreq {
	public static class DocMapper
		extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException{
			
			//String fileName = ((FileSplit)context.getInputSplit()).getPath().getName().toString();
			//String[] afileName = fileName.split("-");
			String afileName[] = value.toString().split("-");
			String fileName = afileName[0];		//Get name of file
			
			//String doc_value = value.toString();
			String doc_value = afileName[1];
			String[] doc_values = doc_value.split("\t");
			String key = doc_values[0];
			
			String key_value = fileName+"-"+doc_values[1];
			
			context.write(new Text(key), new Text(key_value));
		}
	}
	
	public static class DocReducer
		extends Reducer<Text, Text, Text, DoubleWritable>{
		
		private MultipleOutputs<Text, DoubleWritable> multipleoutputs;
		
		public void setup(Context context) throws IOException, InterruptedException{
	    	multipleoutputs = new MultipleOutputs<Text, DoubleWritable>(context);
	    }
		
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			// double total_no_of_docs = Double.parseDouble(context.getJobName());
	double no_of_docs = 0;
			String docs[] = key.toString().split(":");
			double total_no_of_docs = Double.valueOf(docs[1]);
			String word = docs[0];
			HashMap<String, Double> map = new HashMap<String, Double>();
			
			for(Text val : value){
				String[] words = val.toString().split("-");	//"file_name-termFrequency" --> file_name termFrequency
				double tf = 0;
				tf = Double.parseDouble(words[1]);
				String fileName = words[0];
				no_of_docs+=1;
				map.put(fileName, tf);
			}
			for(String wordKey : map.keySet()){
				double idf = total_no_of_docs/no_of_docs;
				double tf_idf = map.get(wordKey)*(java.lang.Math.log(idf));	//tf_idf = tf*log_e(total_no_of_docs/no_of_docs)
				String required = word;
				required = required+"-"+wordKey;
				context.write(new Text(required), new DoubleWritable(tf_idf));
			}
		}
	   /* public void cleanup(Context context) throws IOException, InterruptedException{
	    	multipleoutputs.close();
	    }*/
	}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "DocumentFrequency");
	    
	    job.setJarByClass(InverseDocFreq.class);
	    System.out.println("Class is set");
	    
	    job.setMapperClass(DocMapper.class);
	    job.setCombinerClass(DocReducer.class);
	    job.setReducerClass(DocReducer.class);
	    System.out.println("Mapper reducer class set");
	    

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    System.out.println("Reducer keyvalue class set");
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    System.out.println("Map keyvalue class set");
	    
	    //LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    Path output2 = new Path("/tfidf");
	    FileSystem  hdfs2 = FileSystem.get(conf);
	    if(hdfs2.exists(output2)){
	    	hdfs2.delete(output2, true);
	    }
	    
	    FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9001/TermFrequency/"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9001/tfidf"));
	    
	    Path inputPath = new Path("hdfs://namenode:9001/TermFrequency/");
	    FileSystem fs = inputPath.getFileSystem(conf);
	    FileStatus[] stat = fs.listStatus(inputPath);
	    job.setJobName(String.valueOf(stat.length));
	    System.out.println("File input/output path set");
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}