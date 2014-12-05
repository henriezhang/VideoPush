
package com.webdev.test.news;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DeepBayesPredict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 7) {
            System.err.println("Usage: hadoop jar *.jar com.webdev.test.tubd.DeepBayesPredict <article_info> <hist_click> <sdate> <edate> <test_file> <test_user> <out_path>");
            System.exit(5);
        }

        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", "gboss");
        conf.set("mapred.queue.name", "gboss");
        conf.set("push.news.path", otherArgs[0]); // 历史push文章信息数据路径
        conf.set("push.news.sdate", otherArgs[2]); // 开始时间
        conf.set("push.news.edate", otherArgs[3]); // 结束时间
        conf.set("push.news.test", otherArgs[4]); // 测试文件路径

        Job job = new Job(conf, "BossPushPredict");
        job.setJarByClass(DeepBayesPredict.class);
        job.setMapperClass(DeepBayesPredictMapper.class);
        job.setReducerClass(DeepBayesPredictReducer.class);
        job.setNumReduceTasks(400);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(com.webdev.test.news.UserClick.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定学习数据路径
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date dt1 = formatter.parse(otherArgs[2]);
        Date dt2 = formatter.parse(otherArgs[3]);
        Calendar cd1 = Calendar.getInstance();
        cd1.setTime(dt1);
        int endDs = Integer.parseInt(formatter.format(dt2));
        FileSystem fs = FileSystem.get(conf);
        for (int i = 1; Integer.parseInt(formatter.format(cd1.getTime())) < endDs && i < 360; i++) {
            String tmpPath = otherArgs[1] + "/ds=" + formatter.format(cd1.getTime());
            Path tPath = new Path(tmpPath);
            if (fs.exists(tPath)) {
                FileInputFormat.addInputPath(job, tPath);
                System.out.println("Exist " + tmpPath);
            } else {
                System.out.println("Not exist " + tmpPath);
            }
            cd1.add(Calendar.DATE, 1);
        }

        // 指定输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[6]));
        System.out.println(otherArgs[2]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class DeepBayesPredictMapper
            extends Mapper<LongWritable, Text, Text, UserClick> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            com.webdev.test.news.UserClick userClick = new UserClick();
            try {  // 不能正常处理的记录丢弃之
                if (fields.length >= 3) {
                    userClick.setType(DataType.HISTORY);
                    userClick.setImei(fields[0]);
                    userClick.setAid(fields[1]);
                    userClick.setClick(Integer.parseInt(fields[2]));
                } else if (fields.length == 1) {
                    userClick.setType(DataType.PREDICT);
                    userClick.setImei(fields[0]);
                } else {
                    return;
                }
            } catch (Exception e) {
                System.err.println("failed Item:" + userClick.toString());
                return;
            }
            context.write(new Text(fields[0]), userClick);
        }
    }

    public static class DeepBayesPredictReducer
            extends Reducer<Text, UserClick, Text, Text> {
        // 已push文章信息
        private HashMap<String, DeepArticleInfo> pushNews = new HashMap<String, DeepArticleInfo>();

        private List<DeepArticleInfo> testNews = new LinkedList<DeepArticleInfo>();

        private Pattern sufPattern = null;

        private Matcher sufMatcher = null;

        private String getTrace(Throwable throwable) {
            StringWriter stringWriter = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }

        // 读取需要测试的文章
        private void readTestData(FileSystem fs, String testPath) {
            Path pDir = null;
            FileStatus[] stats = null;
            FSDataInputStream in = null;
            LineReader reader = null;
            try {  // 列出目录下所有文件
                pDir = new Path(testPath);
                stats = fs.listStatus(pDir);
            } catch (Exception e) {
                System.err.println("list dir files failed");
                System.err.println(this.getTrace(e));
                return;
            }
            for (int i = 0; i < stats.length && !stats[i].isDir(); i++) {
                Path fPath = stats[i].getPath();
                try { // 打开文件
                    in = fs.open(fPath);
                } catch (Exception e) {
                    System.err.println("open file failed file=" + fPath.toString());
                    System.err.println(this.getTrace(e));
                    continue;
                }
                reader = new LineReader(in);
                Text test = new Text();
                System.out.println("Read Test data");
                try { // 读取测试数据
                    while (reader.readLine(test) > 0) {
                        DeepArticleInfo aInfo = new DeepArticleInfo();
                        aInfo.setNews(test.toString());
                        this.testNews.add(aInfo);
                        System.out.println(aInfo.getAid() + ":testNewsSize:" + this.testNews.size());
                    }
                } catch (Exception e) {
                    System.err.println("read file failed file=" + fPath.toString());
                    System.err.println(this.getTrace(e));
                    continue;
                }
            }
        }

        // 读取需要学习的文章
        private void readLearnData(FileSystem fs, String newsPath, String newsSdate, String newsEdate) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
            Date dt1 = null;
            Date dt2 = null;
            try {
                dt1 = formatter.parse(newsSdate);
                dt2 = formatter.parse(newsEdate);
            } catch (Exception e) {
                System.err.println("format date failed");
                System.err.println(e.getStackTrace().toString());
                return;
            }
            Calendar cd1 = Calendar.getInstance();
            cd1.setTime(dt1);
            int endDs = Integer.parseInt(formatter.format(dt2));
            FileStatus[] stats = null;
            FSDataInputStream in = null;
            LineReader reader = null;
            System.out.println("Read Learn data");
            for (int t = 1; Integer.parseInt(formatter.format(cd1.getTime())) <= endDs && t < 360; t++) {
                String tmpPath = newsPath + "/ds=" + formatter.format(cd1.getTime());
                cd1.add(Calendar.DATE, 1);
                Path pDir = new Path(tmpPath);
                try {
                    if (!fs.exists(pDir)) {
                        System.out.println("Not exist file path: " + tmpPath);  // debug
                        continue;
                    }
                    System.out.println("Exist file path: " + tmpPath);  // debug
                    stats = fs.listStatus(pDir);
                } catch (Exception e) {
                    System.err.println("list learn data failed");
                    System.err.println(this.getTrace(e));
                    continue;
                }
                for (int i = 0; i < stats.length && !stats[i].isDir(); i++) {
                    Path fPath = stats[i].getPath();
                    try { // 打开文件
                        in = fs.open(fPath);
                    } catch (Exception e) {
                        System.err.println("open file failed file=" + fPath.toString());
                        System.err.println(this.getTrace(e));
                        continue;
                    }
                    reader = new LineReader(in);
                    Text learn = new Text();
                    try { // 读取测试数据
                        while (reader.readLine(learn) > 0) {
                            DeepArticleInfo aInfo = new DeepArticleInfo();
                            aInfo.setNews(learn.toString());
                            this.pushNews.put(aInfo.getAid(), aInfo);
                            System.out.println(aInfo.getAid() + ":pushNewsSize:" + this.pushNews.size());
                        }
                    } catch (Exception e) {
                        System.err.println("read learn data file failed file=" + fPath.toString());
                        System.err.println(this.getTrace(e));
                        continue;
                    }
                }
            }
        }

        // 文章属性信息在计算前加载
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // 获取已push文章存储路径
            String newsPath = conf.get("push.news.path");
            // 数据开始时间
            String newsSdate = conf.get("push.news.sdate");
            // 数据结束时间
            String newsEdate = conf.get("push.news.edate");
            // 读取已push文章信息
            this.readLearnData(fs, newsPath, newsSdate, newsEdate);

            // 获取要预测的文章存储路径
            String testPath = conf.get("push.news.test");
            this.readTestData(fs, testPath);

            this.sufPattern = Pattern.compile("[13579]$");
        }

        //  计算文章和点击类或者非点击类的相似度
        private double similarProbability(DeepArticleInfo pNews, List<UserClick> hist) {
            int histNum = hist.size();
            double similarity = 0.0;
            for (int i = 0; i < histNum; i++) {
                similarity += pNews.similarTo(this.pushNews.get(hist.get(i).getAid()));
            }
            return (histNum == 0) ? 0.0 : (similarity / histNum);
        }

        @Override
        public void reduce(Text uin, Iterable<UserClick> clickItems, Context context)
                throws IOException, InterruptedException {

            // 读取用户行为数据
            List<UserClick> allHist = new Vector<UserClick>();
            for (UserClick item : clickItems) {
                allHist.add(new UserClick(item)); // 一定要重新生成一个copy，否则数据会有丢失
            }

            // 存放点击实例的列表
            List<UserClick> clickHist = new Vector<UserClick>();
            // 存放没有点击实例的的列表
            List<UserClick> noClickHist = new Vector<UserClick>();
            // 需要预测的用户
            UserClick uPredict = null;

            /// 对数据基本处理
            for (UserClick uClick : allHist) {
                int uType = uClick.getType();
                if (uType == DataType.HISTORY) { // click hist
                    int click = uClick.getClick();
                    if (click == UserClickType.CLICK) {
                        clickHist.add(uClick);
                    } else if (click == UserClickType.NOCLICK) {
                        noClickHist.add(uClick);
                    }
                } else if (uType == DataType.PREDICT) {  // user to app
                    uPredict = uClick;
                }
            }

            //this.sufMatcher = this.sufPattern.matcher(uin.toString());
            //if (this.sufMatcher.find()) {
                for(int i = 0; i < this.testNews.size(); i++) {
                    DeepArticleInfo pNews = this.testNews.get(i);
                    // 当前用户跟点击过历史数据相似的概率
                    double clikRate = this.similarProbability(pNews, clickHist);
                    // 当前用户跟未点击过历史数据相似的概率
                    double noClikRate = this.similarProbability(pNews, noClickHist);
                    int predict = (clikRate / noClikRate > 0.8) ? UserClickType.CLICK : UserClickType.NOCLICK;
                    Text result = new Text("" + predict + "\t" + clikRate + "\t" + noClikRate);
                    context.write(uin, result);
                }
            //}
            /*else {
                if(Math.random()<0.3) {
                    context.write(uin, new Text("2"));
                }
            }*/
        }
    }
}