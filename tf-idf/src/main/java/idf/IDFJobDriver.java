package idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class IDFJobDriver extends Configured implements Tool{
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		/*
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        // WindowsLocalFileSystem ±««— º≥¡§ ≈¨∑°Ω∫
        conf.set("fs.file.impl", "index.WindowsLocalFileSystem");
        conf.set("io.serializations",
                  "org.apache.hadoop.io.serializer.JavaSerialization," +
                  "org.apache.hadoop.io.serializer.WritableSerialization");
 */
        Job job = new Job(conf,"Word in Corpus, TF-IDF");
 
        // ∏ ∆€ & ∏Æµ‡º≠
        job.setMapperClass(IDFMapper.class); // ∏ ∆€
//        job.setCombinerClass(IntSumReducer.class); // ƒﬁπŸ¿Ã≥ 
        job.setReducerClass(IDFReducer.class);   // ∏Æµ‡º≠ 
         
        // ¿Œ«≤ æ∆øÙ«≤ º≥¡§
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);        
 
        Path input = new Path("output2");
        Path output = new Path("output3");
 
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        Path inputPath = new Path("input");
        FileSystem fs2 = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs2.listStatus(inputPath);
        job.setJobName(String.valueOf(stat.length));
 
        //ileSystem fs = FileSystem.getLocal(conf);
        //fs.delete(output, true); // ¿Ã¿¸ √‚∑¬ ∆ƒ¿œ ªË¡¶
         
        job.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
