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

public class Search extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Search.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Search(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    
    Scanner scan = new Scanner(System.in);
    System.out.println("Enter Query String");
    String query = scan.nextLine();

    Job job = Job.getInstance(getConf(), "termFreq");
    job.setJarByClass(this.getClass());
    FileSystem fileSystem = FileSystem.get(getConf());
    ContentSummary contentSummary = fileSystem.getContentSummary(new Path(args[0]));
    int docCount = (int)contentSummary.getFileCount();
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("interim_path"));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
	
    Configuration config = new Configuration(getConf());
    config.set("count",String.valueOf(docCount));
    Job job2 = Job.getInstance(config, "tfidf");
    job2.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job2, new Path("interim_path"));
    FileOutputFormat.setOutputPath(job2, new Path("tfidf_path")); //Chaining jobs another way
    job2.setMapperClass(Map2.class); 
    job2.setReducerClass(Reduce2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.waitForCompletion(true);

    Configuration config_search = new Configuration(getConf());
    config_search.set("query",query);
    Job job3 = Job.getInstance(config_search, "search");
    job3.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job3, new Path("tfidf_path"));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
    job3.setMapperClass(Map3.class);
    job3.setReducerClass(Reduce3.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    return job3.waitForCompletion(true) ? 0 : 1;

  }

   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      Text currentWord = new Text();
      for (String word : WORD_BOUNDARY.split(line)) {
        if (word.isEmpty()) {
            continue;
        }
            currentWord = new Text(word.toLowerCase()+"#####"+fileName);
            context.write(currentWord,one);
        }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new DoubleWritable(sum>0? 1+Math.log10(sum): 0.0));
    }
  }

  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {

      String line  = lineText.toString();
      String[] entity = line.split("#####");
      String[] val = entity[1].split("\\s+");
      context.write(new Text(entity[0]),new Text(val[0]+"="+val[1]));
    }
  }

  public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
      int occurances = 0, docCount = Integer.parseInt(context.getConfiguration().get("count"));
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

  public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String[] query_string = context.getConfiguration().get("query").split(" ");
      String line  = lineText.toString();
      String[] entity = line.split("#####");
      String[] val = entity[1].split("\\s+");
      for(int i=0; i<query_string.length; i++){
        if(query_string[i].equals(entity[0])){ // Fetch previously executed tfidf and get matching entries
         context.write(new Text(val[0]),new Text(val[1]));
        } 
      }
    }
  }

  public static class Reduce3 extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
    throws IOException, InterruptedException {
    double sum = 0.0;
    for (Text count : counts) {
	sum+= Double.parseDouble(count.toString());
    }
    context.write(word , new DoubleWritable(sum)); //Provide final ranked scores

    }
  }

}
