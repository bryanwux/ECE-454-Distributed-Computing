import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {
  public static class MovieRatingMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Text movieName = new Text();
    private Text maxIndex = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
      String[] tokens = value.toString().split(",");
      movieName.set(tokens[0]);
      int currentMaxRating = Integer.MIN_VALUE;

      for(int i=1; i<tokens.length; i++){
        if(tokens[i].isEmpty())
          continue;
        int rating = Integer.parseInt(tokens[i]);
        if(rating > currentMaxRating){
          currentMaxRating = rating;
          maxIndex.clear();
          maxIndex.set(String.valueOf(i));
        }else if(rating == currentMaxRating){
          byte[] bytes = String.format(",%s", i).getBytes();
          maxIndex.append(bytes, 0, bytes.length);
        }
      }
      context.write(movieName, maxIndex);
    
    }

  }

    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // add code here
    job.setMapperClass(MovieRatingMapper.class);
//    job.setCombinerClass(MovieRatingCombiner.class);
//    job.setReducerClass(MovieRatingReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
