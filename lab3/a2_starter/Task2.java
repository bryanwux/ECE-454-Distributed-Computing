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
import org.apache.hadoop.io.NullWritable;


public class Task2 {
    // Mapper class
    public static class RatingSumMapper extends Mapper<Object, Text, NullWritable, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String[] stringTokens = value.toString().split(",", -1);

            for(int i=1; i<stringTokens.length; i++) {
                if(stringTokens[i].equals(""))
                    continue;
                context.write(NullWritable.get(), one);
            }
        }
    }

    // Reducer class, basically same as word count example
    public static class RatingSumReducer extends Reducer<NullWritable,IntWritable,NullWritable, IntWritable>{
        private IntWritable result=new IntWritable();

        public void reduce(NullWritable key,Iterable<IntWritable> values,Context context)
                throws IOException,InterruptedException
        {
            int sum=0;
            for(IntWritable val : values){
                sum+=val.get();
            }
            result.set(sum);
            context.write(NullWritable.get(), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task2.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        job.setMapperClass(RatingSumMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(RatingSumReducer.class);
        job.setReducerClass(RatingSumReducer.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}