package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.SparkSession;

public class RelevantDocumentSQL {
	
	public static void main(String args[]){
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession session = SparkSession.builder().appName("Tfidf").master("spark://namenode:7077").getOrCreate();
		
		DataFrameReader dataFrameReader = session.read();
		
		Dataset<Row> responses = dataFrameReader.option("header", "true").csv("hdfs://namenode:9001/all-the-news/");
		
		//Get the result of id and content
		Dataset<Row> requiredData = responses.select("id", "content");
		
		Long docCount = requiredData.count();
		
		//*********************UDF function defined***********************//
		UDF1<Long, Double> hello = new UDF1<Long, Double>() {
			   /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			   public Double call(Long df) throws Exception {
					//long docCount = 3;
			       return Math.log((Double.valueOf(docCount) + 1) / (Double.valueOf(df) + 1));
			   }
			};
		//***********************UDF function end***********************//
		//Show dataset of file
		//requiredData.show();
		
		RegexTokenizer regexTokenizer = new RegexTokenizer()
			    .setInputCol("content")
			    .setOutputCol("words")  
				.setPattern("\\w+").setGaps(false);
		
		Dataset<Row> wordsData = regexTokenizer.transform(requiredData.na().fill("")).select("id","content", "words");
		
		//Print data with it's keywords...
		//id	contents	words
		//wordsData.show();
		
		//Splitting Every Single word to Individual column
		Dataset<Row> temp = wordsData.select("id","content", "words").withColumn("word", org.apache.spark.sql.functions.explode(wordsData.col("words")));
		
		//List of every single word
		// id	content		words	word
		//temp.show();
		
		//Printing every single word
		/*for(Row row:temp.collectAsList()){
			System.out.println(row.get(0) + ", " + row.get(1) + ", " + row.get(2));
		}*/
		
		//calculating total words in an individual file
		Dataset<Row> totalWords = temp.groupBy("id").count().withColumnRenamed("count", "total_words");
		
		//Showing totalWords
		//id	total_words
		//totalWords.show();
		
		totalWords = totalWords.join(wordsData, "id");
		
		//Calculating Frequency
		Dataset<Row> tokensFrequency = temp.groupBy("id", "word").count().withColumnRenamed("count", "frequency");
		
		//Showing Frequency of Each Word
		//id word Frequency
		//tokensFrequency.show();
		
		//************ADDING JOIN OPERATION OF totalWords and tokensFrequency to get Term Frequency
		Dataset<Row> total_words_with_frequency = totalWords.join(tokensFrequency, "id");
		Dataset<Row> termFrequency = total_words_with_frequency.withColumn("term_frequency", total_words_with_frequency.col("frequency")
				.divide(total_words_with_frequency.col("total_words")));
		
		//Showing Term Frequency of Each Word
		//id	total_words		word	frequency	term_frequency
		termFrequency.show();
		
		
		//Calculating Document Frequency
		Dataset<Row> tokensWithDF = termFrequency.groupBy("word").count().withColumnRenamed("count", "doc_frequency");
		
		//Document Frequency
		//word	 doc_frequency
		//tokensWithDF.show();

		session.udf().register("idf", hello, DataTypes.DoubleType);

		//Calculating Inverse Document Frequency
		Dataset<Row> tokensWithIDF = tokensWithDF.withColumn("idf", org.apache.spark.sql.functions.callUDF("idf", tokensWithDF.col("doc_frequency")));
		
		//Inverse Document Frequency values
		//word	doc_frequency	idf
		tokensWithIDF.show();
		 
		Dataset<Row> idf = termFrequency.join(tokensWithIDF, "word").withColumn("tf-idf", termFrequency.col("term_frequency").multiply(tokensWithIDF.col("idf")));
		
		//TF-IDF values
		//word	id 	total_words	frequency  term_frequency doc_frequency	idf	tf-idf
		idf.select("id", "word", "tf-idf").orderBy(idf.col("word").desc()).show();
		
		//Query starts
		String choice;
		do{
			System.out.println("1. Enter query to search document");
			System.out.println("2. Exit");
			Scanner sc = new Scanner(System.in);
			choice = sc.nextLine();
			if(choice.equals("1")){
				System.out.println("Enter query");
				String query = sc.nextLine();
				
				//************Add multiple query code here****************//
				List<String> myList = new ArrayList<String>(Arrays.asList(query.split(" ")));
				Dataset<Row> document = idf.filter(idf.col("word")
						.isin(myList.stream().toArray(String[]::new)))
						.select("id","word","tf-idf", "content")
						.orderBy(idf.col("tf-idf").desc());
				
				//document.groupBy("id").sum("tf-idf").show();
				
				document.limit(2).show();
				System.out.println("Want more result y/n");
				choice = sc.nextLine();
				if(choice.equals("y")){
					document.limit(5).show();
				}
				System.out.println("Want to enter query in the same result???y/n");
				choice = sc.nextLine();
				if(choice.equals("y")){
					query = sc.nextLine();
					myList = new ArrayList<String>(Arrays.asList(query.split(" ")));
					idf.filter(idf.col("word").isin(myList.stream().toArray(String[]::new))).limit(2).show();	//
				}
				System.out.println("Want to next query or exit q/e");
				choice = sc.nextLine();
			}
		}while(choice.equals("q")); 
	}
}
