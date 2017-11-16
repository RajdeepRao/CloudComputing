
package org.myorg;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class LinkMapper extends Mapper<LongWritable, Text, Text, Text> {
      
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
        String line = lineText.toString();
        Pattern pattern;
        Matcher matcher;
        try{

	  if(line!=null && !line.trim().equals("")){
		pattern = Pattern.compile("<title>(.+?)</title>");
		matcher = pattern.matcher(line);
		matcher.find();
		String title = matcher.group(1);

		pattern = Pattern.compile("<text>(.+?)</title>");
		matcher = pattern.matcher(line);
		matcher.find();
		String contents = matcher.group(1);

		pattern = Pattern.compile("\\[\\[.*?]\\]");
		matcher = pattern.matcher(contents);
		
		while(matcher.find()){
			String link = matcher.group().replace("[[","").replace("]]","");
			if(link.length()==0){
				context.write(new Text(link), new Text(title.trim()));
			}
		}
		  
	  }
        }
        catch (Exception e){
		return;
        }
	
      
    }
  }

