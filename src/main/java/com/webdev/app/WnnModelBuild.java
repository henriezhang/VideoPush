package com.webdev.app;

import com.webdev.entity.Constants;
import com.webdev.entity.UserAction;
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
import java.util.List;
import java.util.Vector;

/**
 * Created by henriezhang on 2014/11/5.
 */
public class WnnModelBuild {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar VideoPush.jar com.webdev.app.WnnModelBuild <click_hist> <out_path>");
            System.exit(6);
        }

        // 参数获取
        String clickHist = otherArgs[0];
        String outPath = otherArgs[1];

        Job job = new Job(conf, "WnnModelBuild");
        job.setJarByClass(WnnModelBuild.class);
        job.setMapperClass(WnnModelBuildMapper.class);
        job.setReducerClass(WnnModelBuildReducer.class);
        job.setNumReduceTasks(600);

        // the map output is Text, VideoClick
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserAction.class);

        // the reduce output is Text, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定学习数据路径
        Path uPath = new Path(clickHist);
        FileInputFormat.addInputPath(job, uPath);

        // 指定输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WnnModelBuildMapper
            extends Mapper<LongWritable, Text, Text, UserAction> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\u0001"); // \u0001
            // guid 合法性判断
            if (fields[0].length() != 32) {
                return;
            }

            if (fields.length >= 3) {
                UserAction vc = new UserAction();
                vc.setId(fields[1]);
                vc.setVisitType(fields[2]);
                vc.setCnt(fields[3]);
                vc.setVec(fields[4]);
                context.write(new Text(fields[0]), vc);
            }
        }
    }

    public static class WnnModelBuildReducer
            extends Reducer<Text, UserAction, Text, Text> {
        // 计算视频的均值向量
        private double[] statSimiVector(List<UserAction> hist) {
            double vec[] = new double[Constants.VECLEN];
            if (hist == null || hist.size() == 0) {
                return vec;
            }

            // 累加各个维度的权值
            double v;
            int histSize = hist.size();
            for (int i = 0; i < histSize; i++) {
                String tmp = hist.get(i).getVec();
                String[] fields = tmp.split(",");
                for (int j = 0; j < Constants.VECLEN && j < fields.length; j++) {
                    try {
                        v = Double.parseDouble(fields[j]);
                        vec[j] += v;
                    } catch (Exception e) {
                    }
                }
            }

            // 求取最终向量
            if (histSize > 0) {
                for (int i = 0; i < Constants.VECLEN; i++) {
                    vec[i] /= histSize;
                }
            }
            return vec;
        }

        // 将权值数组组成字符串
        private String arrToString(double arr[]) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < Constants.VECLEN; i++) {
                sb.append(" ");
                //sb.append(String.format("%.4f", arr[i]));
                sb.append(Double.parseDouble(String.format("%.4f", arr[i])));
            }
            return sb.substring(1);
        }

        @Override
        public void reduce(Text uin, Iterable<UserAction> clickItems, Context context)
                throws IOException, InterruptedException {
            List<UserAction> allHist = new Vector<UserAction>();
            for (UserAction item : clickItems) {
                allHist.add(new UserAction(item)); // 一定要重新生成一个copy，否则数据会有丢失
            }

            // 存放点击实例的列表
            List<UserAction> clickHist = new Vector<UserAction>();
            // 存放没有点击实例的的列表
            List<UserAction> noClickHist = new Vector<UserAction>();

            // 对历史数据分类
            for (UserAction vc : allHist) {
                int visitType = vc.getVisitType();
                if (visitType == UserAction.CLICK || visitType == UserAction.VISIT) {
                    clickHist.add(vc);
                } else if (visitType == UserAction.NOCLICK) {
                    noClickHist.add(vc);
                }
            }

            // 计算没有用户点击视频的向量和用户不点击视频的向量
            double simiVec[] = this.statSimiVector(clickHist);
            double noSimiVec[] = this.statSimiVector(noClickHist);
            if (simiVec.length == Constants.VECLEN && noSimiVec.length == Constants.VECLEN) {
                String simiStr = arrToString(simiVec);
                String noSimiStr = arrToString(noSimiVec);
                context.write(uin, new Text(simiStr + " " + noSimiStr));
            } else {
                System.err.println("invalid vector");
            }
            // context.write(uin, new Text(clickHist.size() + " " + noClickHist.size()));
        }
    }
}