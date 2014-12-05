
package com.webdev.test.tubd;

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
import java.util.*;

public class KeywordsPredict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 3) {
            System.out.println(otherArgs.length);
            System.err.println("Usage: hadoop jar *.jar com.webdev.test.tubd.KeywordsPredict <keywords_path> <news_Path> <out_path>");
            System.exit(3);
        }

        String keyworsPath = otherArgs[0];
        String newsPath = otherArgs[1];
        String outPath = otherArgs[2];

        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", "gboss");
        conf.set("mapred.queue.name", "gboss");
        conf.set("push.news.path", newsPath); // 需要push的文章信息

        Job job = new Job(conf, "UserKeywordsPredict");
        job.setJarByClass(KeywordsPredict.class);
        job.setMapperClass(KeywordsPredictMapper.class);
        job.setReducerClass(KeywordsPredictReducer.class);
        job.setNumReduceTasks(0);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        // 指定学习数据路径
        Path iPath = new Path(keyworsPath);
        FileInputFormat.setInputPaths(job, iPath);

        // 指定输出文件路径
        Path oPath = new Path(outPath);
        // 如果输出路径已经存在则清除之
        if (fs.exists(oPath)) {
            fs.deleteOnExit(oPath);
        }
        FileOutputFormat.setOutputPath(job, oPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class KeywordsPredictMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, HashMap<String, Integer>> pushNews = new HashMap<String, HashMap<String, Integer>>();

        //  计算文章和点击类或者非点击类的相似度
        private double statSimilarity(HashMap<String, Integer> user, HashMap<String, Integer> article) {
            if (user == null || article == null || user.size() == 0 || article.size() == 0) {
                return 0.0;
            }
            HashSet<String> words = new HashSet<String>();
            words.addAll(user.keySet());
            words.addAll(article.keySet());

            double muldot = 0.0;
            double square1 = 0.0;
            double square2 = 0.0;
            for (String word : words) {
                Integer W1 = user.get(word);
                Integer W2 = article.get(word);
                double w1 = (W1 == null) ? 0 : W1.intValue();
                double w2 = (W2 == null) ? 0 : W2.intValue();
                muldot += w1 * w2;
                square1 += w1 * w1;
                square2 += w2 * w2;
            }
            return muldot / (Math.sqrt(square1 + 0.0001) * Math.sqrt(square2 + 0.0001));
        }

        private String getTrace(Throwable throwable) {
            StringWriter stringWriter = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }

        private final void setKeyword(Map<String, Integer> map, String key) {
            if (key != "") {
                Integer value = map.get(key);
                value = (value == null) ? 1 : value + 1;
                map.put(key, value);
            }
        }

        private void statTopics(Map<String, Integer> map, String value) {
            String[] keywords = value.split(",", 20);
            for (int i = 0; i < keywords.length; i++) {
                //解析关键字及权重值
                String item[] = keywords[i].split(":", 2);
                String key = item[0];
                this.setKeyword(map, key);
            }
        }

        // 处理标题关键词
        private void statTitleTopics(Map<String, Integer> map, String value) {
            String[] keywords = value.split(",", 20);
            for (int i = 0; i < keywords.length; i++) {
                this.setKeyword(map, keywords[i]);
            }
        }

        // 处理编辑打的tag
        private void statTags(Map<String, Integer> map, String value) {
            String[] tags = value.split(",", 20);
            for (int i = 0; i < tags.length; i++) {
                this.setKeyword(map, tags[i]);
            }
        }

        // 文章属性信息在计算前加载
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // 获取已push文章存储路径
            String newsPath = conf.get("push.news.path");
            Path pDir = null;
            FileStatus[] stats = null;
            FSDataInputStream in = null;
            LineReader reader = null;
            try {  // 列出目录下所有文件
                pDir = new Path(newsPath);
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
                try { // 读取测试数据
                    while (reader.readLine(test) > 0) {
                        String[] item = test.toString().split("\t");
                        String aid = item[0];
                        String topic = item[5];
                        String titleTopic = item[6];
                        //String tags = item[7];

                        HashMap<String, Integer> keywords = new HashMap<String, Integer>();
                        this.statTopics(keywords, topic);
                        this.statTitleTopics(keywords, titleTopic);
                        //this.statTags(keywords, tags);

                        this.pushNews.put(aid, keywords);
                    }
                } catch (Exception e) {
                    System.err.println("read file failed file=" + fPath.toString());
                    System.err.println(this.getTrace(e));
                    continue;
                }
            }
        }

        private  HashMap<String, Integer> getUserKeywords(String value)
        {
            HashMap<String, Integer> user = new  HashMap<String, Integer>();
            String[] keywords = value.split(",", 500);
            for (int i = 0; i < keywords.length; i++) {
                //解析关键字及权重值
                String item[] = keywords[i].split(":", 2);
                String key = item[0];
                int val = 1;
                try {
                    val = Integer.parseInt(item[1]);
                } catch(Exception e) {
                    System.err.println("parse failed:"+keywords[i]);
                }
                user.put(key, val);
            }
            return user;
        }

        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            // 对学习和测试数据取样
            if (fields[0].length() > 2 && fields[0].charAt(fields[0].length() - 1) != '2') {
                return;
            }

            // 计算用户关键字
            HashMap<String, Integer> user = getUserKeywords(fields[1]);

            // 循环处理测试的文章
            Iterator<String> keySetIterator = this.pushNews.keySet().iterator();
            while (keySetIterator.hasNext()) {
                String aid = keySetIterator.next();
                HashMap<String, Integer> article = this.pushNews.get(key);

                double similarity = this.statSimilarity(user, article);
                //long simi = Math.round(similarity*100000);
                context.write(new Text(fields[0]+"\t"+aid), new Text(""+similarity));
            }
            //context.write(new Text(fields[0]+"\ttest"), new Text("test:"+this.pushNews.size()));
        }
    }

    public static class KeywordsPredictReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text uin, Iterable<Text> items, Context context)
                throws IOException, InterruptedException {

        }
    }
}