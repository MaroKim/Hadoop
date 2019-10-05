package index;

import org.apache.hadoop.util.ToolRunner;

import idf.IDFJobDriver;
import tf.TFJobDriver;
import wordcount.WordCountJobDriver;
public class MainClass {
	public static void main(String[] args) throws Exception{
		
		ToolRunner.run(new WordCountJobDriver(), null);
		ToolRunner.run(new TFJobDriver(), null);
		ToolRunner.run(new IDFJobDriver(), null);
		
	}

}
