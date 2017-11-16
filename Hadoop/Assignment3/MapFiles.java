
package org.myorg;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

public static class MapFiles extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      context.write(new Text("Placeholder"),new IntWritable(1));
    }
  }

