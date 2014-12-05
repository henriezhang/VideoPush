
package com.webdev.test.tubd;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UserKeywords {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", "gboss");
        conf.set("mapred.queue.name", "gboss");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 5) {
            System.err.println("Usage: hadoop jar BossBayesPredict.jar com.webdev.test.tubd.UserKeywords <tfrom> <in_path> <out_path> <sdate> <edate>");
            System.exit(5);
        }
        String tFrom = otherArgs[0];
        String inPath = otherArgs[1];
        String outPath = otherArgs[2];
        String sDate = otherArgs[3];
        String eDate = otherArgs[4];

        Job job = new Job(conf, "UserKeywords");
        job.setJarByClass(UserKeywords.class);
        job.setMapperClass(UserKeywordsMapper.class);
        job.setReducerClass(UserKeywordsReducer.class);
        job.setNumReduceTasks(500);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定学习数据路径
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date dt1 = formatter.parse(sDate);
        Date dt2 = formatter.parse(eDate);
        Calendar cd1 = Calendar.getInstance();
        cd1.setTime(dt1);
        int endDs = Integer.parseInt(formatter.format(dt2));
        FileSystem fs = FileSystem.get(conf);
        for (int i = 1; Integer.parseInt(formatter.format(cd1.getTime()))<=endDs && i < 360; i++) {
            String tmpPath = otherArgs[1] + "/ds=" + formatter.format(cd1.getTime()) + "/tfrom=" + tFrom;
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
        String tmpPath = outPath + "/ds=" + endDs + "/tfrom=" + tFrom;
        System.out.println("out put path:"+tmpPath);
        Path tPath = new Path(tmpPath);
        // 如果输出路径已经存在则清除之
        if(fs.exists(tPath)) {
            fs.deleteOnExit(tPath);
        }
        FileOutputFormat.setOutputPath(job, tPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UserKeywordsMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t", 6);
            if (fields.length < 6) {
                return;
            }
            // 输出标题关键字，编辑标签，文章关键字,访问时间
            //Text outText = new Text(fields[3] + "\t" + fields[5] + "\t"  + "\t" + fields[4] + "\t" + fields[2]);
            Text outText = new Text(fields[3] + "\t" + fields[5]);
            context.write(new Text(fields[0]), outText);
        }
    }

    public static class UserKeywordsReducer
            extends Reducer<Text, Text, Text, Text> {
        // 访问时间数据
        private Map<String, Integer> vTimes = new HashMap<String, Integer>();
        // 标题关键词数据
        private Map<String, Integer> titleTopics = new HashMap<String, Integer>();
        // 关键词数据
        private Map<String, Integer> topics = new HashMap<String, Integer>();
        // Tags数据
        private Map<String, Integer> tags = new HashMap<String, Integer>();

        private Map<String, Integer> keywords = new HashMap<String, Integer>();

        // 将关键字加入hashmap中
        private final void setKeyword(Map<String, Integer> map, String key) {
            Integer value = map.get(key);
            value = (value==null) ? 1 : value + 1;
            map.put(key, value);
        }

        // 处理计算时间
        private void statTimes(String value) {
            int hourTag = -1;
            // 将访问时间处理成一周中的第x个小时
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date dt = formatter.parse(value);
                Calendar cd = Calendar.getInstance();
                cd.setTime(dt);
                hourTag = cd.DAY_OF_WEEK * 24 + cd.HOUR_OF_DAY;
            } catch (Exception e) {
                return;
            }
            // 如果是可解析的时间，将时间存入hash表中
            if(hourTag!=-1) {
                this.setKeyword(this.vTimes, "" + hourTag);
            }
        }

        // 处理标题关键词
        private void statTitleTopics(String value) {
            String[] keywords = value.split(",", 20);
            for (int i = 0; i < keywords.length; i++) {
                //this.setKeywords(this.titleTopics, keywords[i]);
                this.setKeyword(this.keywords, keywords[i]);
            }
        }

        private void statTopics(String value) {
            String[] keywords = value.split(",", 20);
            for (int i = 0; i < keywords.length; i++) {
                //解析关键字及权重值
                String item[] = keywords[i].split(":", 2);
                String key = item[0];

                // 将权重值
                //this.setKeywords(this.topics, key);
                this.setKeyword(this.keywords, key);
            }
        }

        // 处理编辑打的tag
        private void statTags(String value) {
            String[] tags = value.split(",", 20);
            for (int i = 0; i < tags.length; i++) {
                //this.setKeywords(this.tags, tags[i]);
                this.setKeyword(this.keywords, tags[i]);
            }
        }

        // 返回排序的权重值
        private final String getSortedValues(Map<String, Integer> map) {
            // 复制到数组
            List<Map.Entry<String, Integer>> keyIds =
                    new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
            // 排序
            Collections.sort(keyIds, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return (o2.getValue() - o1.getValue());
                }
            });

            // 输出排序结果
            StringBuilder sb = new StringBuilder();
            // 依次读出关键字和权重并且最多取200个关键字
            for(int i=0; i<keyIds.size() && i<200; i++) {
                sb.append(",");
                sb.append(keyIds.get(i).getKey());
                sb.append(":");
                sb.append(keyIds.get(i).getValue());
            }

            // 去掉第一个逗号返回
            return (sb.length()>1) ? sb.substring(1) : "haha";
        }

        // 处理计算时间
        private String getTimes() {
            return this.getSortedValues(this.vTimes);
        }

        // 处理标题关键词
        private String getTitleTopics() {
            return this.getSortedValues(this.titleTopics);
        }

        // 处理关键词
        private String getTopics() {
            return this.getSortedValues(this.topics);
        }

        // 处理编辑打的tag
        private String getTags() {
            return this.getSortedValues(this.tags);
        }

        // 处理标题关键词
        private String getKeywords() {
            return this.getSortedValues(this.keywords);
        }

        public void reduce(Text uin, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            /// 读取用户行为数据
            //Text user = new Text(uin);
            List<String> viewHist = new Vector<String>();
            for (Text item : inValues) {
                viewHist.add(item.toString()); // 一定要重新生成一个copy，否则数据会有丢失
            }

            /// 对数据进行处理
            for (String item : viewHist) {
                String[] fields = item.split("\t", 2);
                this.statTitleTopics(fields[0]);
                this.statTags(fields[1]);
            }

            // 输出关键字
            String keywords = this.getKeywords();
            context.write(uin, new Text(keywords));
        }
    }
}