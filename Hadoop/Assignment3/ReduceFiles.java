
package org.myorg;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

public static class ReduceFiles extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) { //Iterate over received counts for each key and calculate sum for each key.
        sum += count.get();
      }
      context.write(word, new IntWritable(sum)); //Print to output Key followed by the number of times it has occured.
    }
  }

