/*
Name: Rajdeep Rao
EmailID: rrao6@uncc.edu
800#: 800972470
*/
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.lang.*;
import java.util.*;
import java.text.*;

import org.apache.log4j.Logger;

public class TFIDF extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(TFIDF.class);

  public static void main(String[] args) throws Exception {
    String[] tempArgs = new String[args.length];
    System.arraycopy(args, 0, tempArgs, 0, args.length);
    tempArgs[1] = "/user/cloudera/termFrequency_interim_path";
    int res = ToolRunner.run(new TermFrequency(), tempArgs); //Chaining Jobs to run them one after another
    res = ToolRunner.run(new TFIDF(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    FileSystem fileSystem = FileSystem.get(getConf());
    ContentSummary contentSummary = fileSystem.getContentSummary(new Path(args[0]));
    int docCount = (int)contentSummary.getFileCount(); // Calculating total number of documents in the input folder
    Configuration config = new Configuration(getConf());
    config.set("count",String.valueOf(docCount)); // Store the count of documents as a config parameter so it's accessible in the reduce job to calc IDF
    Job job2 = Job.getInstance(config, "tfidf");
    job2.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job2, new Path("termFrequency_interim_path"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    return job2.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {

      String line  = lineText.toString();
      String[] entity = line.split("#####"); // String manipulation to re organise keys
      String[] val = entity[1].split("\\s+");
      context.write(new Text(entity[0]),new Text(val[0]+"="+val[1]));
    }
  }

  public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
      int occurances = 0, docCount = Integer.parseInt(context.getConfiguration().get("count")); // Accessing previously stored count to calculate IDF
      double idf;
      List<String> keys = new ArrayList<String>();
      for (Text count : counts) {
	occurances++;
	String[] entity = count.toString().split("=");
        keys.add(word.toString()+"#####"+entity[0]+"="+entity[1]);
      }
      idf = Math.log10(1+(docCount/occurances));
      for(int i=0; i<keys.size(); i++){
	String key = keys.get(i);
	try{
	     NumberFormat f = NumberFormat.getInstance();
	     double tf = f.parse(key.split("=")[1]).doubleValue();		
	     double answer = tf*idf;
	     context.write(new Text(key.split("=")[0]) , new DoubleWritable(answer));		
		}
	catch(Exception e){}
      }
    }
  }
}
