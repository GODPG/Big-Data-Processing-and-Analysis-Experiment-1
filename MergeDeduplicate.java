import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MergeDeduplicate {


    public static class MergeMapper extends Mapper<Object, Text, Text, NullWritable> 
    {
        private static NullWritable nullValue = NullWritable.get();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            context.write(value, nullValue);
        }
    }


    public static class MergeReducer extends Reducer<Text, NullWritable, Text, NullWritable> 
    {
        private static NullWritable nullValue = NullWritable.get();
        
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException 
        {
            context.write(key, nullValue);
        }
    }


    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        Job job = Job.getInstance(conf, "Merge and Deduplicate");
        job.setJarByClass(MergeDeduplicate.class);
        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath = new Path("/user/hadoop-wei/merge_input");
        Path outputPath = new Path("/user/hadoop-wei/merge_output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean isSuccess = job.waitForCompletion(true);
        System.exit(isSuccess ? 0 : 1);
    }
}