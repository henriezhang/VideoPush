package com.webdev.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by henriezhang on 2014/12/10.
 * Function 计算每个用户观看的专辑的列表
 */
public class UserVisitCovers {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 6) {
            System.err.println("Usage: hadoop jar UserVisitCovers <hist_in> <real_in> <out> <sdate> <edate> <tag>");
            System.exit(6);
        }

        String histIn = otherArgs[0];
        String realIn = otherArgs[1];
        String out = otherArgs[2];
        String sDate = otherArgs[3];
        String eDate = otherArgs[4];

        Job job = new Job(conf, "UserVisitCovers");
        job.setJarByClass(UserVisitCovers.class);
        job.setMapperClass(UserVisitCoversMapper.class);
        job.setReducerClass(UserVisitCoversReducer.class);
        job.setNumReduceTasks(400);

        // the map output is IntWriteable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWriteable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        ParsePosition pos = new ParsePosition(0);
        Date dt = formatter.parse(otherArgs[5], pos);
        Calendar cd = Calendar.getInstance();
        cd.setTime(dt);
        FileSystem fs = FileSystem.get(conf);
        for (int i = 0; i < 10; i++) {
            String tmpPath = histIn + "/ds=" + formatter.format(cd.getTime() + "/tfrome=aphone");
            Path tPath = new Path(tmpPath);
            if (fs.exists(tPath)) {
                FileInputFormat.addInputPath(job, tPath);
                System.out.println("Exist " + tmpPath);
            } else {
                System.out.println("Not exist " + tmpPath);
            }
            cd.add(Calendar.DATE, -1);
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UserVisitCoversMapper
            extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text inValue, Context context
        ) throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\u0001", 3);
            if (fields.length < 3) {
                return;
            }

            // 输出每个用户观看的专辑及次数
            if (fields[0].length() == 32 && fields[1].length() == 15) {
                context.write(new Text(fields[0]), new Text(fields[1] + ":" + fields[2]));
            }
        }
    }

    public static class UserVisitCoversReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text uin, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text val : values) {
                sb.append(",");
                sb.append(val.toString());
            }

            // 检查是否有记录
            if (sb.length() == 0) {
                return;
            }

            // 输出每个用户的观看列表
            context.write(new Text(uin), new Text(sb.substring(1)));
        }
    }
}