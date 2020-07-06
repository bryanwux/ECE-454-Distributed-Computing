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

public class Task2 {

  // mapper class
  public static class RatingNumMapper extends Mapper<Object, Text, NullWritable, IntWritable>{
    private IntWritable one = new Intwritable(1);
    private NullWritable key = NullWritable.get();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
      String[] stringToken = value.toString().split(",");
      for (int i = 1; i< stringToken.length; i++){
        if(!stringToken[i].isEmpty()){
          context.write(key, one);
        }else{
          continue;
        }
      }

    }
  }

  // reducer class, basically same as wordcount example

  public static class RatingNumReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable>{
    private IntWritable result = new IntWritable();

    public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
      int sum = 0;
      for(IntWritable val: values){
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    conf.set("mapreduce.output.textoutputformat.separator", ",");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


    Job job = Job.getInstance(conf, "Task2");

    job.setJarByClass(Task2.class);

    job.setMapperClass(RatingNumMapper.class);
    job.setCombinerClass(RatingNumReducer.class);
    job.setReducerClass(RatingNumReducer.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
