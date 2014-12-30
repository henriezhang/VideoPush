package com.webdev.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
        if (otherArgs.length < 4) {
            System.err.println("Usage: hadoop jar RecdCovers <in> <out> <topn> <tag>");
            System.exit(4);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];
        String topn = otherArgs[2];
        conf.set("cover.cf.topn", topn);

        Job job = new Job(conf, "RecdCovers");
        job.setJarByClass(RecdCovers.class);
        job.setMapperClass(RecdCoversMapper.class);
        job.setCombinerClass(RecdCoversCombiner.class); // combiner 本地合并优化计算
        job.setReducerClass(RecdCoversReducer.class);
        job.setNumReduceTasks(400);

        // the map output is IntWriteable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWriteable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
                context.write(new Text(fields[0]), new Text(fields[1]));
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
                int tmpv = 0;
                try {
                    tmpv = Integer.parseInt(items[1]);
                } catch (Exception e) {
                }
                if (!coverMap.containsKey(items[0])) {
                    coverMap.put(items[0], tmpv);
                } else {
                    int sum = Integer.parseInt(coverMap.get(items[0]).toString());
                    coverMap.put(items[0], sum + tmpv);
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
                int tmpv = 0;
                try {
                    tmpv = Integer.parseInt(items[1]);
                } catch (Exception e) {
                }
                if (!coverMap.containsKey(items[0])) {
                    coverMap.put(items[0], tmpv);
                } else {
                    int sum = Integer.parseInt(coverMap.get(items[0]).toString());
                    coverMap.put(items[0], sum + tmpv);
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
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : arrayList) {
                if (topn-- < 0) {
                    break;
                }
                sb.append(",");
                sb.append(entry.getKey());
                sb.append(":");
                sb.append(entry.getValue());
            }
            if(sb.length()>1) {
                context.write(id, new Text(sb.substring(1)));
            }
        }
    }
}