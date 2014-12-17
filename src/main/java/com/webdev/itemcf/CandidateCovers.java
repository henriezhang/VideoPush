package com.webdev.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by henriezhang on 2014/12/10.
 */
public class CandidateCovers {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 4) {
            System.err.println("Usage: hadoop jar CandidateCovers <in_pref> <in_pair> <out> <tag>");
            System.exit(4);
        }

        String inPref = otherArgs[0];
        String inPair = otherArgs[1];
        String out = otherArgs[2];

        Job job = new Job(conf, "CandidateCovers");
        job.setJarByClass(CandidateCovers.class);
        job.setMapperClass(CandidateCoversMapper.class);
        job.setReducerClass(CandidateCoversReducer.class);
        job.setNumReduceTasks(400);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set input and output
        FileInputFormat.setInputPaths(job, new Path(inPref));
        FileInputFormat.addInputPath(job, new Path(inPair));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class CandidateCoversMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private Pattern pUin;
        private Matcher mUin;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            pUin = Pattern.compile(":");
        }

        /*
         * map:取出收中的qq和对应的url,qq对应的group
         */
        @Override
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            if (fields.length == 2) {
                mUin = pUin.matcher(fields[0]);
                if (mUin.find()) { // 专辑同好对数据
                    if (fields[0].length() == 31) { // 31为coverid+:+coverid长度
                        String covers[] = fields[0].split(":");
                        if (covers.length == 2) {
                            context.write(new Text(covers[0]), new Text(covers[1] + ":" + fields[1]));
                        }
                    }
                } else { // 用户评分数据
                    if (fields[0].length() == 32) { // 32为uin长度
                        String prefs[] = fields[1].split(",");
                        for (String pref : prefs) {
                            String item[] = pref.split(":");
                            if (item.length == 2) {
                                context.write(new Text(item[0]), new Text(fields[0] + ":" + item[1]));
                            }
                        }
                    }
                }
            }
        }
    }

    public static class CandidateCoversReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text id, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text uin = null;
            int score = 0;
            HashMap coverMap = new HashMap<String, Integer>();
            for (Text item : values) {
                String fields[] = item.toString().split(":");
                if (fields.length == 2) {
                    if (fields[0].length() == 32) { // 用户评分数据
                        uin = new Text(fields[0]);
                        score = Integer.parseInt(fields[1]);
                    } else if (fields[0].length() == 15) { // 同好对数据
                        coverMap.put(fields[0], fields[1]);
                    }
                }
            }

            if (uin != null) { // 输出数据
                Iterator hit = coverMap.entrySet().iterator();
                while (hit.hasNext()) {
                    Map.Entry entry = (Map.Entry) hit.next();
                    String coverId = (String) entry.getKey();
                    int value = 0;
                    try {
                        value = score * ((Integer) entry.getValue()).intValue();
                    } catch (Exception e) {
                    }
                    context.write(uin, new Text(coverId + ":" + value));
                } // end while
            }
        }
    }
}
