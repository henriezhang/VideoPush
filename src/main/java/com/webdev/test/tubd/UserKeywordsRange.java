
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

public class UserKeywordsRange {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", "gboss");
        conf.set("mapred.queue.name", "gboss");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 5) {
            System.err.println("Usage: hadoop jar BossBayesPredict.jar com.webdev.test.tubd.UserKeywordsRange <tfrom> <in_path> <out_path> <sdate> <edate>");
            System.exit(5);
        }
        String tFrom = otherArgs[0];
        String inPath = otherArgs[1];
        String outPath = otherArgs[2];
        String sDate = otherArgs[3];
        String eDate = otherArgs[4];

        Job job = new Job(conf, "UserKeywordsRange");
        job.setJarByClass(UserKeywordsRange.class);
        job.setMapperClass(UserKeywordsRangeMapper.class);
        job.setReducerClass(UserKeywordsRangeReducer.class);
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

    public static class UserKeywordsRangeMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t", 2);
            if (fields.length < 1) {
                return;
            }

            // 输出关键字
            Text outText = new Text(fields[1]);
            context.write(new Text(fields[0]), outText);
        }
    }

    public static class UserKeywordsRangeReducer
            extends Reducer<Text, Text, Text, Text> {
        // 用户关键字
        private Map<String, Integer> keywords = new HashMap<String, Integer>();

        // 将关键字加入hashmap中
        private final void setKeyword(Map<String, Integer> map, String key, int addValue) {
            if(key!="") {
                Integer value = map.get(key);
                value = (value == null) ? addValue : value + addValue;
                map.put(key, value);
            }
        }

        private void statKeywords(String valueStr) {
            String[] keywords = valueStr.split(",", 20);
            for (int i = 0; i < keywords.length; i++) {
                //解析关键字及权重值
                String item[] = keywords[i].split(":", 2);
                String key = item[0];
                int value = 1;
                try {
                    value = Integer.parseInt(item[1]);
                } catch(Exception e) {
                    System.err.println("Parse value failed!value="+keywords[i]);
                }

                // 将权重值
                this.setKeyword(this.keywords, key, value);
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
                this.statKeywords(fields[0]);
            }

            // 输出关键字
            String keywords = this.getKeywords();
            context.write(uin, new Text(keywords));
        }
    }
}