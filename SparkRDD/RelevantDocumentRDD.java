package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;

public class RelevantDocumentRDD {
		
		public static void main(String args[]){
			SparkConf sparkConf = new SparkConf().setAppName("Relevant Document").setMaster("spark://namenode:7077");
			JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
			
			JavaRDD<String> inputFile = sparkContext.textFile("hdfs://namenode:9001/all-the-news/");
			
			//inputFile = inputFile.filter(row -> row.split());
			
			//Gives total document
			Broadcast<Long> total_docs = sparkContext.broadcast(inputFile.count());
			
			//Extract id and article
			JavaRDD<String> data = inputFile.map(line -> {
				String column_data[] = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				int size = column_data.length;
				if(column_data[0] == null)
					column_data[0] = " ";
				return StringUtils.join(new String[] {column_data[0], column_data[size-1]}, ",");
			});
			
			//Extract single word     hello's how! are. you"s -> hello s	how
			JavaRDD<String> tokens = data.flatMap(line -> {
				String column[] = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				String token[] = column[1].split("\\W");
				for(int i=0;i<token.length;++i){
					token[i] = token[i]+"-"+column[0]+"-"+token.length;		//<word>-<filename>-<total_words in doc>
				}
				return Arrays.asList(token).iterator();
			});
			
			//<word>-<filename>-<total_words in doc> 1
			JavaPairRDD<String, Integer> mapping = tokens.mapToPair(line -> {
				return new Tuple2<String, Integer>(line, 1);
			});
			
			//Gives word count
			JavaPairRDD<String, Integer> frequency = mapping.reduceByKey((x, y) -> x+y);
			
			//
			JavaPairRDD<String, String> termFrequency = frequency.mapToPair(line -> {
				String token[] = line._1().split("-");
				double total_words = Double.valueOf(token[2]);	//Total words
				String key = token[0];	//word
				
				double tf_double = Double.valueOf(line._2())/total_words;
				String tf_string = String.valueOf(tf_double);
				String value = token[1]+"-"+tf_string;	//<filename>-<term_frequency>
				return new Tuple2<String, String>(key, value);
			});
			
			//
			JavaPairRDD<String, Iterable<String>> idf = termFrequency.groupByKey();
			
			JavaRDD<String> tf_idf = idf.flatMap(line -> {
				Iterator<String> str = line._2().iterator();
				double no_of_doc = 0;
				HashMap<String, Double> map = new HashMap<String, Double>();
				
				while(str.hasNext()){
					String values[] = str.next().toString().split("-");
					double tf_value = Double.valueOf(values[1]);
					map.put(values[0], tf_value);
					no_of_doc++;
				}
				ArrayList<String> k = new ArrayList<String>();
				for(String wordKey : map.keySet()){
					double idf_value = Double.valueOf(total_docs.getValue()+1.0)/(no_of_doc+1);
					System.out.println(idf_value+","+map.get(wordKey));
					double tf_idf_value = Double.valueOf(map.get(wordKey))*(java.lang.Math.log(idf_value));
					k.add(line._1+":"+wordKey+"-"+(tf_idf_value));
				}
				return k.iterator();
			});
			
			
		System.out.println("Enter query to search document");
		Scanner scanner = new Scanner(System.in);
		String query = scanner.nextLine();
		Broadcast<String> broadcast_query = sparkContext.broadcast(query);
		
		JavaRDD<String> searching = tf_idf.filter(line -> {
			// line contains keyWord:filename-tf_idf
			String token[] = line.split(":");
			String keyWord = token[0];
			//String filename_tfidf[] = token[1].split("-");
			String match_query[] = broadcast_query.getValue().split(" ");
			for(int i=0;i<match_query.length;++i){
				System.out.println(match_query[i]+" "+keyWord);
				if(match_query[i].equals(keyWord)){
					System.out.println("Enter");
					return true;//new Tuple2<String, Double>(match_query[i]+":"+filename_tfidf[0], Double.valueOf(filename_tfidf[1]));
				}
			}
			return false;
		});
		
		JavaPairRDD<String, Double> results = searching.mapToPair(line -> {
			// line contains keyWord:filename-tf_idf
			String token[] = line.split(":");
			String required_word[] = token[1].split("-");
			String key = required_word[0];
			String value = required_word[1];
			return new Tuple2<String, Double>(key, Double.valueOf(value));
		}).reduceByKey((x, y) -> (x+y));
		
		for(Tuple2<String, Double> content : results.collect()){
			System.out.println(content._1 + " " + content._2);
		}
		//results.saveAsTextFile("hdfs://namenode:9001/rdd_results");
		}
}