package com.custom.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;


public class IterativeBalancerJob {

    //核心Mapper
    public static class FGBMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        private Map<String, Integer> dic = new HashMap<>(); 
        private long[] s;                                   
        
        private Map<String, Integer> fgbBuffer = new HashMap<>();
        private int currentFgbRecords = 0;
        private static final int FGB_MAX_SIZE = 5000; 
        private int numReducers;

        @Override
        protected void setup(Context context) 
        {
            numReducers = context.getNumReduceTasks();
            if (numReducers <= 0) numReducers = 1;
            s = new long[numReducers]; 
            System.err.println("=== [Paper-MP] Mapper 启动，Reducer 数量: " + numReducers + " ===");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String word = value.toString().trim();
            if (word.isEmpty()) return;

            fgbBuffer.put(word, fgbBuffer.getOrDefault(word, 0) + 1);
            currentFgbRecords++;

            if (currentFgbRecords >= FGB_MAX_SIZE) 
            {
                flushFGB(context);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {
            if (!fgbBuffer.isEmpty()) 
            {
                flushFGB(context);
            }
            System.err.println("=== [Paper-MP] Mapper 执行完毕，最终负载分布: " + Arrays.toString(s) + " ===");
        }

        //第二个算法实现
        private void flushFGB(Context context) throws IOException, InterruptedException 
        {
            List<Map.Entry<String, Integer>> newKeys = new ArrayList<>();
            
            for (Map.Entry<String, Integer> mp : fgbBuffer.entrySet())
            {
                String k = mp.getKey();
                int freq = mp.getValue();
                
                if (dic.containsKey(k)) 
                {
                    int assignedReducer = dic.get(k);
                    s[assignedReducer] += freq;
                } 
                else 
                {
                    newKeys.add(mp);
                }
            }

            newKeys.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            for (Map.Entry<String, Integer> mp : newKeys) 
            {
                String k = mp.getKey();
                int freq = mp.getValue();

                // 寻找最小负载的 Reducer (S_min)
                int minReducer = 0;
                long minLoad = s[0];
                for (int i = 1; i < numReducers; i++)
                {
                    if (s[i] < minLoad)
                    {
                        minLoad = s[i];
                        minReducer = i;
                    }
                }

                dic.put(k, minReducer);
                s[minReducer] += freq;
            }

            IntWritable one = new IntWritable(1);
            for (Map.Entry<String, Integer> mp : fgbBuffer.entrySet()) 
            {
                String k = mp.getKey();
                int freq = mp.getValue();
                int targetReducer = dic.get(k);
                
                Text routedKey = new Text(k + "|" + targetReducer);
                
                // 逐条发射
                for (int i = 0; i < freq; i++) {
                    context.write(routedKey, one);
                }
            }

            fgbBuffer.clear();
            currentFgbRecords = 0;
        }
    }

    public static class DynamicRouterPartitioner extends Partitioner<Text, IntWritable> 
    {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions)
        {
            String keyStr = key.toString();
            int lastPipeIndex = keyStr.lastIndexOf("|");
            if (lastPipeIndex != -1) 
            {
                try {
                    int target = Integer.parseInt(keyStr.substring(lastPipeIndex + 1));
                    return target % numPartitions; 
                } catch (NumberFormatException e) {
                    // Fallback
                }
            }
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static class RestorationReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            result.set(sum);
            
            // 还原真实的 Key (去掉 "|ID")
            String keyStr = key.toString();
            int lastPipeIndex = keyStr.lastIndexOf("|");
            if (lastPipeIndex != -1)
            {
                keyStr = keyStr.substring(0, lastPipeIndex);
            }
            
            context.write(new Text(keyStr), result);
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Iterative Data Partitioning");
        job.setJarByClass(IterativeBalancerJob.class);

        // 设置Mapper, Reducer和自定义的Partitioner
        job.setMapperClass(FGBMapper.class);
        job.setPartitionerClass(DynamicRouterPartitioner.class);
        job.setReducerClass(RestorationReducer.class);

        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
