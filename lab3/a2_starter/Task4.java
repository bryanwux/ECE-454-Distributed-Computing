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

public class Task4 {
  public static class MovieSimilarityMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Text movieName;
    private IntWritable movieSimilarity; 
  
  // called once at the beginning of each task
  @Override
  public void setup(Context context)
    throws IOException, InterruptedException{
      BufferedReader reader = new BufferedReader(new FileReader(DISTRIBUTED_CACHE_LABEL));
      cache.clear();
      String line;
      while ((line = reader.readLine()) != null) cache.add(line);
  }

  public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException{
      String[] tokens = value.toString().split(",");
      String title = tokens[0];



    }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    System.out.println(otherArgs);
    // add code here
    job.setMapperClass(Task4.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
