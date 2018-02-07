package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriter {
	  public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
		  Path p = new Path(args[0]);
		  File f = new File("/home/training/training_materials/data/testlog/test_access_log");
		  SequenceFile.Writer writer = null;
		  Text key = new Text();
		  Text value = new Text();
		  String[] aline;
		  try{
			  writer = SequenceFile.createWriter(fs,conf,p,key.getClass(),value.getClass());
		  
			try {
				FileReader fileReader = new FileReader(f);

//				Scanner sc = new Scanner(fileReader);
//				while(sc.hasNext()){
//					String line = sc.next();
//					aline = line.split(" - - ");
//					key.set(aline[0]);
//					value.set(aline[1]);
//					writer.append(key, value);
//					
//				}
//				sc.close();
				
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					  aline = line.split(" - - ");
					  key.set(aline[0]);
					  value.set(line);
					  writer.append(key, value);
				}
				bufferedReader.close();
				fileReader.close();
			} 
		  	catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }finally{
			  IOUtils.closeStream(writer);
		  }
	  }
}
