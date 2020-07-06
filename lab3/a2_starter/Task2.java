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

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable>{
        private final static intWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String[] tokens = value.toString().split(",");

            for(int i=1; i<tokens.length; i++) {
                if(tokens[i].isEmpty())
                    continue;
                context.write(one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,IntWritable>{
        private IntWritable result=new IntWritable();

        public void reduce(Text key,iterable<IntWritable> values,Context context)
                throws IOException,InterruptedException
        {
            int sum=0;
            for(IntWritable val : values){
                sum+=val.get();
            }
            result.set(sum);
            context.write(result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task2.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);


        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}