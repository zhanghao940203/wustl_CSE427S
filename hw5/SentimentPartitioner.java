package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();

  /**
   * Set up the positive and negative hash set in the setConf method.
   */
  @Override
  public void setConf(Configuration configuration) {
     /*
     * Add the positive and negative words to the respective sets using the files 
     * positive-words.txt and negative-words.txt.
     */
    /*
     * TODO implement 
     */
	  this.configuration = configuration;
	  
//	  File positivewords = new File("/home/training/workspace/wordcount/src/stubs/positive-words.txt");
//	  File negativewords = new File("/home/training/workspace/wordcount/src/stubs/negative-words.txt");
	  File positivewords = new File("positive-words.txt");
	  File negativewords = new File("negative-words.txt");
	  
	try {
		FileReader fileReaderp = new FileReader(positivewords);

		Scanner scp = new Scanner(fileReaderp);
		while(scp.hasNext()){
			String pword = scp.next();
			if(!(pword.charAt(0)==';')){
				positive.add(pword);
			}
		}
		scp.close();
		fileReaderp.close();
	} 
  	catch (FileNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	try {
		FileReader fileReadern = new FileReader(negativewords);

		Scanner scn = new Scanner(fileReadern);
		while(scn.hasNext()){
			String nword = scn.next();
			if(!(nword.charAt(0)==';')){
				negative.add(nword);
			}
		}
		scn.close();
		fileReadern.close();
	} 
  	catch (FileNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
//	try {
//		FileReader fileReaderp = new FileReader(positivewords);
//		BufferedReader bufferedReaderp = new BufferedReader(fileReaderp);
//		  String positiveline;
//		  while ((positiveline = bufferedReaderp.readLine()) != null) {
//			  if (!(positiveline.charAt(0) == ';')){
//				  positive.add(positiveline);
//			  }
//		  }
//		  bufferedReaderp.close();
//		  fileReaderp.close();
//	} catch (FileNotFoundException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
	  
//	  
//	try {
//		FileReader fileReadern = new FileReader(negativewords);
//		BufferedReader bufferedReadern = new BufferedReader(fileReadern);
//		  String negativeline;
//		  while ((negativeline = bufferedReadern.readLine()) != null) {
//			  if (!(negativeline.charAt(0) == ';')){
//				  negative.add(negativeline);
//			  }
//		  }
//		  bufferedReadern.close();
//		  fileReadern.close();	
//	} catch (FileNotFoundException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//	   
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
	  if (positive.contains(key.toString())){
		  return 0;
	  }
	  else if (negative.contains(key.toString())){
		  return 1;
	  }
	  else{
		  return 2;
	  }
  }
}
