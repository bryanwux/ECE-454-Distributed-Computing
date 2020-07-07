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

public class Task3 {

  // Mapper class
  public static class RatingPerUserMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private  IntWritable id = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
      String[] stringTokens = value.toString().split(",", -1);

      for(int i=1; i<stringTokens.length; i++) {
        id.set(i);
        if(stringTokens[i].isEmpty(){
          context.write(id, zero);
        context.write(id, one);
      }
    }
  }

    
  // Reducer class
  public static class RatingPerUserReducer extends Reducer<IntWritable,IntWritable,IntWritable, IntWritable>{
    private IntWritable result=new IntWritable();

    public void reduce(IntWritable key,Iterable<IntWritable> values,Context context)
            throws IOException,InterruptedException
    {
      int sum=0;
      for(IntWritable val : values){
        sum+=val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(RatingPerUserMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setCombinerClass(RatingPerUserReducer.class);
    job.setReducerClass(RatingPerUserReducer.class);


    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
