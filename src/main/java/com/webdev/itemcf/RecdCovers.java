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
import java.util.*;

/**
 * Created by henriezhang on 2014/12/10.
 */
public class RecdCovers {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 6) {
            System.err.println("Usage: hadoop jar RecdCovers <in> <out> <topn> <tag>");
            System.exit(6);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];
        String topn = otherArgs[2];
        conf.set("cover.cf.topn", otherArgs[4]);

        Job job = new Job(conf, "RecdCovers");
        job.setJarByClass(RecdCovers.class);
        job.setMapperClass(RecdCoversMapper.class);
        job.setCombinerClass(RecdCoversCombiner.class); // combiner 本地合并优化计算
        job.setReducerClass(RecdCoversReducer.class);
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

    public static class RecdCoversMapper
            extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text inValue, Context context
        ) throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            if (fields.length >= 2) {
                context.write(new Text(fields[0]), new Text(fields[2]));
            }
        }
    }

    public static class RecdCoversCombiner
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap coverMap = new HashMap<String, Integer>();
            for (Text val : values) {
                String items[] = val.toString().split(":");
                if (!coverMap.containsKey(items[0])) {
                    coverMap.put(items[0], Integer.parseInt(items[1]));
                } else {
                    Integer sum = (Integer) coverMap.get(items[0]);
                    coverMap.put(items[0], sum + items[1]);
                }
            }

            Iterator iter = coverMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                context.write(id, new Text(entry.getKey() + ":" + entry.getValue()));
            }
        }
    }

    public static class RecdCoversReducer
            extends Reducer<Text, Text, Text, Text> {
        private static int topnFlag = 20;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            topnFlag = Integer.parseInt(conf.get("cover.cf.topn"));
        }

        @Override
        public void reduce(Text id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap coverMap = new HashMap<String, Integer>();
            for (Text val : values) {
                String items[] = val.toString().split(":");
                if (!coverMap.containsKey(items[0])) {
                    coverMap.put(items[0], Integer.parseInt(items[1]));
                } else {
                    Integer sum = (Integer) coverMap.get(items[0]);
                    coverMap.put(items[0], sum + items[1]);
                }
            }

            ArrayList<Map.Entry<String, Integer>> arrayList = new ArrayList<Map.Entry<String, Integer>>(coverMap.entrySet());
            Collections.sort(arrayList, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> e1,
                                   Map.Entry<String, Integer> e2) {
                return (e2.getValue()).compareTo(e1.getValue());
                }
            });

            int topn = topnFlag;
            for (Map.Entry<String, Integer> entry : arrayList) {
                if (topn-- < 0) {
                    break;
                }
                context.write(id, new Text(entry.getKey() + "|" + entry.getValue()));
            }
        }
    }
}