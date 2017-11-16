/*
Name: Rajdeep Rao
EmailID: rrao6@uncc.edu
800#: 800972470
*/
package org.myorg;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Arrays;



import org.apache.log4j.Logger;

public class Driver extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Driver.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Driver(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    
    int n=0,i=0;
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);
    Path linkPath = new Path("pagerank/output/links");

    if(hdfs.exists(linkPath))
	hdfs.delete(linkPath, true);

    Path totalPath = new Path("pagerank/output/total"); // Delete intermediate paths if they exist
    if(hdfs.exists(totalPath))
	hdfs.delete(totalPath, true);
    
    //This job maps a file and all of it's out links
    Job job = Job.getInstance(conf, "Link");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0])); //Path for input files
    FileOutputFormat.setOutputPath(job, linkPath); //Path for output files
    job.setMapperClass(LinkMapper.class);
    job.setReducerClass(LinkReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    job.waitForCompletion(true);

    //This job calculates the number of files present
    Job job1 = Job.getInstance(conf, "Total");
    job1.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job1, new Path(args[0])); //Path for input files
    FileOutputFormat.setOutputPath(job1, totalPath); //Path for output files
    job1.setMapperClass(TotalMap.class);
    job1.setReducerClass(TotalReduce.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setNumReduceTasks(1);
    job1.waitForCompletion(true);
    
    if(hdfs.exists(new Path(args[1]+i)))
	hdfs.delete(new Path(args[1]+i), true);

    
    try{
	BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path("pagerank/output/total/part-r-00000"))));
	n = Integer.parseInt(br.readLine().split("\t")[1]);
    }
    catch (Exception e){
    }
    conf.set("TotalPages",Integer.toString(n)); // Storing total number of files such that it is accessible by all jobs, as a context variable

    //This job does the first pass to calculate inital page rank using the number of files from the previous job
    Job job2 = Job.getInstance(conf, "InitRank");
    job2.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job2, new Path(args[0])); //Path for input files
    FileOutputFormat.setOutputPath(job2, new Path(args[1]+i)); //Path for output files
    job2.setMapperClass(InitPageRankMap.class);
    job2.setReducerClass(InitPageRankReduce.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.setNumReduceTasks(1);
    job2.waitForCompletion(true);

    //Running the pagerank code for 10 iterations
    int result=0;
    if(job2.waitForCompletion(true)){
	while(i<10){
		//Deleting intermediate directories
		if(hdfs.exists(new Path(args[1]+(i-1) )))
			hdfs.delete(new Path(args[1]+(i-1)) , true);

		Job job3 = Job.getInstance(conf, "PageRank");
		job3.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job3, new Path(args[1]+i)); //Path for input files
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+(i+1))); //Path for output files
		job3.setMapperClass(PageRankMap.class);
		job3.setReducerClass(PageRankReduce.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setNumReduceTasks(4);
		i++;
		job3.waitForCompletion(true);
	}
    }

    // This job is responsible for the clean up phase
    Job job4 = Job.getInstance(conf, "Sort");
    job4.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job4, new Path(args[1]+i)); //Path for input files
    FileOutputFormat.setOutputPath(job4, new Path(args[1])); //Path for output files
    job4.setMapperClass(SortMap.class);
    job4.setReducerClass(SortReduce.class);
    job4.setOutputKeyClass(DoubleWritable.class);
    job4.setOutputValueClass(Text.class);
    job4.setNumReduceTasks(1); // Using just one reducer for the cleanup phase to sort and beautify the output
    job4.setSortComparatorClass(LongWritable.DecreasingComparator.class);// Sorting the output in descending order
    result = job4.waitForCompletion(true)?0:1;

    if(hdfs.exists(new Path(args[1]+9)))
	hdfs.delete(new Path(args[1]+9), true);

    if(hdfs.exists(new Path(args[1]+10)))
	hdfs.delete(new Path(args[1]+10), true); //Deleting remaining intermediate paths

    return result;
  }

  /*    This class reads the input file line by line and maps the title of every text file with all the outgoing links
	Input : <Title, Page Content>
	Output: <Link, Title>
  */
  public static class LinkMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
        String line = lineText.toString();
        Pattern pattern;
        Matcher matcher;
       // try{

	  if(line!=null && !line.trim().equals("")){
		pattern = Pattern.compile("<title>(.+?)</title>"); // Get the title and page contents
		matcher = pattern.matcher(line);
		matcher.find();
		String title = matcher.group(1);
		
		pattern = Pattern.compile("<text(.+?)</text>");
		matcher = pattern.matcher(line);
		matcher.find();
		String contents = matcher.group(1);

		pattern = Pattern.compile("\\[\\[.*?]\\]");
		matcher = pattern.matcher(contents);
		
		while(matcher.find()){
			String link = matcher.group().replace("[[","").replace("]]","");
						
			if(link.length()!=0){
				context.write(new Text(link), new Text(title.trim()));	//write the link and the title
			}
		}
		  
	  }
        //}
        //catch (Exception e){
	//	return;
        //}
	
      
    }
  }


  /*    This class reads the input from the mapper and puts it all together
	Input : <link, all the pages as an Iterable>
	Output: <Link, list of pages it is contained in>
  */  
  public static class LinkReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text word, Iterable<Text> titles, Context context)
        throws IOException, InterruptedException {

        boolean first = true;
	StringBuilder sb = new StringBuilder();
	StringBuilder sb1 = new StringBuilder();

	for(Text title: titles){
		if(first==true){
			sb.append(title);
			first = false;		
		}		
		else
			sb.append("#####"+title);
	}

	String[] titlesArray = sb.toString().split("#####");
	Arrays.sort(titlesArray); // Doing this to sort the pages alphabetically
	first = true;
	for(String title:titlesArray){
		if(first==true){
			sb1.append(title);
			first = false;		
		}		
		else
			sb1.append("#####"+title);
	}
	context.write(word,new Text(sb1.toString()));
    }
  }

  
  /*    This class reads the input file line by line and counts the number of lines => No of files
	Input : <Title, Page Content>
	Output: <Placeholder, 1>
  */
  public static class TotalMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
        String line = lineText.toString();
	if(line!=null && !line.trim().equals("")){
		context.write(new Text("PlaceHolder"), new IntWritable(1));
	}
    }
  }


  /*    This class reads the input from the mapper and adds them all up
	Input : <Placeholder, 1>
	Output: <Placeholder, sum>
  */

  public static class TotalReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
	int sum=0;
	
	for(IntWritable count: counts)
		sum++;
	context.write(word, new IntWritable(sum));
    }
  }	


  /*    This class reads the input file line by line and sets the inital page rank for each page from the context variable that stores the output from 		the previous job.

	Input : <Title, Page Content>
	Output: <Title, Rank+Outlinks>
  */
  public static class InitPageRankMap extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
        String line = lineText.toString();
        Pattern pattern;
        Matcher matcher;
        //try{

	  if(line!=null && !line.trim().equals("")){
		pattern = Pattern.compile("<title>(.+?)</title>");
		matcher = pattern.matcher(line);
		matcher.find();
		String title = matcher.group(1);
		
		pattern = Pattern.compile("<text(.+?)</text>");
		matcher = pattern.matcher(line);
		matcher.find();
		String contents = matcher.group(1);

		pattern = Pattern.compile("\\[\\[.*?]\\]");
		matcher = pattern.matcher(contents);
		
		boolean first = true;
		StringBuilder sb = new StringBuilder();

		while(matcher.find()){
			String link = matcher.group().replace("[[","").replace("]]","");
			if(first==true){
				sb.append(link);
				first = false;		
			}		
			else
				sb.append("#####"+link); // List of all outlinks being created

		}
		double total = Integer.parseInt(context.getConfiguration().get("TotalPages"));
		context.write(new Text(title.trim()), new Text(String.valueOf(1.0/total)+"@@@@@"+sb.toString()));//Calculating and emitting the initial 		page rank
		  
	  }
        //}
        //catch (Exception e){
	//	return;
        //}
	
      
    }
  }


  /*    This class reads the input from the mapper and passes it right on
	Input : <Title, Rank+Outlinks>
	Output: <Title, Rank+Outlinks>
  */
  public static class InitPageRankReduce extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text word, Text rank, Context context)
        throws IOException, InterruptedException {
	context.write(word, rank);
    }
  }	

  /*    This class reads the input from the initial page rank job and writes the rank contributed by each of the pages
	It also emits the title and all its outgoing links
	Input : <Title, Rank+Outlinks>
	Output: <Title, Rank>
	Output: <Title, Outlinks>
  */
  public static class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
        
	try{
		String[] line = lineText.toString().split("\t");
		String title = line[0];
		String[] rankLinks = line[1].split("@@@@@");
		double rank = Double.parseDouble(rankLinks[0]);
		String[] links = rankLinks[1].split("#####");
		
		for(int i=0; i<links.length; i++){
			context.write(new Text(links[i]), new Text("rank@@@@@"+String.valueOf(rank/links.length)) ); // Write rank contributed by each page
		}
		context.write(new Text(title), new Text("links@@@@@"+rankLinks[1]) ); // Write the outlinks
	}
	catch(Exception e){
		//context.write(new Text("Exception"), new Text(String.valueOf(e)) );
	}
    }
  }

  /*    This class reads the input from the mapper and aggregates and calculates one round of page rank
	
	Input : <Title, Rank/Outlinks (Iterable)>
	Output: <Title, New page rank+Outlinks>
  */
  public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text word, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
	double sum=0.0, d=0.85;
	StringBuilder sb = new StringBuilder();
	for(Text val:values){
		String[] vals = val.toString().split("@@@@@");
		if(vals[0].equals("rank")){
			sum+=Double.parseDouble(vals[1]);
		}
		else{
			sb.append(vals[1]);
		}
	}
	double pageRank = (1-d)+(d*sum); // Page rank calculation
	context.write(word,new Text(String.valueOf(pageRank)+"@@@@@"+sb.toString()));
    }
  }	
  /*    This class essentially cleans up the data and makes it nice and presentble
	Input : <Link, Rank+Outlinks>
	Output: <Rank, links>
  */
  public static class SortMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
        
	try{
		String[] line = lineText.toString().split("\t");
		String title = line[0];
		String[] rankLinks = line[1].split("@@@@@");
		double rank = Double.parseDouble(rankLinks[0]);
		
		context.write(new DoubleWritable(rank), new Text(title) );
	}
	catch(Exception e){
		//context.write(new Text("Exception"), new Text(String.valueOf(e)) );
	}
    }
  }

  /*    This sorts it in descending order
	Input : <Rank, links(Iterable)>
	Output: <Link, Rank>
  */
  public static class SortReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    
    public void reduce(DoubleWritable rank, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

	for(Text val:values){
		context.write(new Text(val), rank);
	}
    }
  }	

}
