package com.webdev.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by henriezhang on 2014/12/10.
 * Function：计算专辑的匹配对数
 */
public class CoVisitCoverPairs {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 6) {
            System.err.println("Usage: hadoop jar CoVisitCoverPairs <in> <out> <tag>");
            System.exit(6);
        }

        String in = otherArgs[0];
        String out= otherArgs[1];

        Job job = new Job(conf, "CoVisitCoverPairs");
        job.setJarByClass(CoVisitCoverPairs.class);
        job.setMapperClass(CoVisitCoverPairsMapper.class);
        job.setCombinerClass(CoVisitCoverPairsReducer.class); // combiner 本地合并优化计算
        job.setReducerClass(CoVisitCoverPairsReducer.class);
        job.setNumReduceTasks(400);

        // the map output is IntWriteable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // the reduce output is IntWriteable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class CoVisitCoverPairsMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text inValue, Context context
        ) throws IOException, InterruptedException {
            String[] raw = inValue.toString().split("\t");
            String[] fields = raw[1].toString().split(",");
            if (fields.length > 0) { // 至少观看过一个视频
                HashSet setCover = new HashSet();
                for (String obj : fields) {
                    String[] item = obj.split(":");
                    if (item.length == 2 && item[0].length() == 15) {
                        setCover.add(item[0]);
                    }
                }

                Iterator it1 = setCover.iterator();
                while (it1.hasNext()) {
                    String cover1 = (String) it1.next();
                    Iterator it2 = setCover.iterator();
                    while (it2.hasNext()) {
                        String cover2 = (String) it2.next();
                        if (!cover1.equals(cover2)) { // 自己和自己不匹配
                            context.write(new Text(cover1 + ":" + cover2), new IntWritable(1));
                        }
                    }
                }
            }
        }
    }

    public static class CoVisitCoverPairsReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text id, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(id, new IntWritable(sum));
        }
    }
}