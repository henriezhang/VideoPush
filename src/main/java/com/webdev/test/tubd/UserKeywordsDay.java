
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
import java.util.*;

public class UserKeywordsDay {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", "gboss");
        conf.set("mapred.queue.name", "gboss");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar BossBayesPredict.jar com.webdev.test.tubd.UserKeywordsDay <in_path> <out_path>");
            System.exit(2);
        }
        String inPath = otherArgs[0];
        String outPath = otherArgs[1];

        Job job = new Job(conf, "UserKeywordsDay");
        job.setJarByClass(UserKeywordsDay.class);
        job.setMapperClass(UserKeywordsDayMapper.class);
        job.setReducerClass(UserKeywordsDayReducer.class);
        job.setNumReduceTasks(500);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        // 指定学习数据路径
        Path iPath = new Path(inPath);
        FileInputFormat.setInputPaths(job, iPath);

        // 指定输出文件路径
        Path oPath = new Path(outPath);
        // 如果输出路径已经存在则清除之
        if(fs.exists(oPath)) {
            fs.deleteOnExit(oPath);
        }
        FileOutputFormat.setOutputPath(job, oPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UserKeywordsDayMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t", 6);
            if (fields.length < 6) {
                return;
            }

            // 输出标题关键字，编辑标签
            Text outText = new Text(fields[3] + "\t" + fields[5]);
            context.write(new Text(fields[0]), outText);
        }
    }

    public static class UserKeywordsDayReducer
            extends Reducer<Text, Text, Text, Text> {
        private Map<String, Integer> keywords = new HashMap<String, Integer>();

        // 将关键字加入hashmap中
        private final void setKeyword(Map<String, Integer> map, String key) {
            if(key!="") {
                Integer value = map.get(key);
                value = (value == null) ? 1 : value + 1;
                map.put(key, value);
            }
        }

        // 处理标题关键词
        private void statTitleTopics(String value) {
            String[] keywords = value.split(",", 20);
            for (int i = 0; i < keywords.length; i++) {
                this.setKeyword(this.keywords, keywords[i]);
            }
        }

        // 处理编辑打的tag
        private void statTags(String value) {
            String[] tags = value.split(",", 20);
            for (int i = 0; i < tags.length; i++) {
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
            // 依次读出关键字和权重并且最多取100个关键字
            for(int i=0; i<keyIds.size() && i<100; i++) {
                if(!keyIds.get(i).getKey().equals("") && keyIds.get(i).getKey()!=null) {
                    sb.append(",");
                    sb.append(keyIds.get(i).getKey());
                    sb.append(":");
                    sb.append(keyIds.get(i).getValue());
                }
            }

            // 去掉第一个逗号返回
            return (sb.length()>1) ? sb.substring(1) : "haha";
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