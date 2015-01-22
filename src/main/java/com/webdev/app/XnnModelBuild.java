package com.webdev.app;

import com.webdev.entity.UserAction;
import com.webdev.entity.XnnModelItem;
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

/**
 * Created by henriezhang on 2014/11/5.
 */
public class XnnModelBuild {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 6) {
            System.err.println("Usage: hadoop jar VideoPush.jar com.webdev.app.XnnModelBuild <cover_info> <hist_click> <hist_visit> <sdate> <edate> <tfrom> <out_path>");
            System.exit(6);
        }

        // 参数获取
        String coversInfo = otherArgs[0];
        String histClick = otherArgs[1];
        String histVisit = otherArgs[2];
        String sDate = otherArgs[3];
        String eDate = otherArgs[4];
        String tfrom = otherArgs[5];
        String outPath = otherArgs[6];

        // 设置push所需信息
        conf.set("push.video.coversinfo", coversInfo); // 历史push专辑信息数据路径
        conf.set("push.video.sdate", sDate); // 开始时间
        conf.set("push.video.edate", eDate); // 结束时间

        Job job = new Job(conf, "DnnModelBuild");
        job.setJarByClass(XnnModelBuild.class);
        job.setMapperClass(XnnModelBuildMapper.class);
        job.setReducerClass(XnnModelBuildReducer.class);
        job.setNumReduceTasks(400);

        // the map output is Text, VideoClick
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserAction.class);

        // the reduce output is Text, Text
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
        for (int i = 1; Integer.parseInt(formatter.format(cd1.getTime())) <= endDs && i < 360; i++) {
            // 添加Push点击数据
            String tmpClick = histClick + "/ds=" + formatter.format(cd1.getTime()) + "/tfrom=" + tfrom;
            Path tPath = new Path(tmpClick);
            if (fs.exists(tPath)) {
                FileInputFormat.addInputPath(job, tPath);
                System.out.println("Exist " + tmpClick);
            } else {
                System.out.println("Not exist " + tmpClick);
            }

            // 添加观看时长数据
            String tmpVisit = histVisit + "/ds=" + formatter.format(cd1.getTime()) + "/tfrom=" + tfrom;
            Path tVisit = new Path(tmpVisit);
            if (fs.exists(tVisit)) {
                FileInputFormat.addInputPath(job, tVisit);
                System.out.println("Exist " + tmpVisit);
            } else {
                System.out.println("Not exist " + tmpVisit);
            }
            cd1.add(Calendar.DATE, 1);
        }

        // 指定输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class XnnModelBuildMapper
            extends Mapper<LongWritable, Text, Text, UserAction> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String value = inValue.toString();
            String[] fields = null;
            UserAction ua = new UserAction();
            // 数据拆分
            if (value.indexOf('\t') > 0) { // Push 点击数据
                fields = value.split("\t");
                ua.setId(fields[1]);
                ua.setVisitType(fields[2]);
            } else { // 观看时长数据
                fields = value.split("\u0001");
                ua.setId(fields[1]);
                ua.setVisitType(UserAction.VISIT);
            }

            // 数据输出
            if (fields[0].length() != 32) {
                return;
            }
            context.write(new Text(fields[0]), ua);
        }
    }

    public static class XnnModelBuildReducer
            extends Reducer<Text, UserAction, Text, Text> {
        // 已push文章信息
        private HashMap<String, XnnModelItem> coversInfo = new HashMap<String, XnnModelItem>();

        private String getTrace(Throwable throwable) {
            StringWriter stringWriter = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }

        // 读取需要学习的文章
        private void readCoversInfo(FileSystem fs, String coverPath) {
            FileStatus[] stats = null;
            FSDataInputStream in = null;
            LineReader reader = null;
            System.out.println(coverPath);
            Path pDir = new Path(coverPath);
            try {
                stats = fs.listStatus(pDir);
            } catch (Exception e) {
                System.err.println("list learn data failed");
                System.err.println(this.getTrace(e));
                System.exit(12);
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
                try { // 历史数据
                    while (reader.readLine(test) > 0) {
                        XnnModelItem cInfo = new XnnModelItem();
                        cInfo.setVecBuild(test.toString());
                        this.coversInfo.put(cInfo.getId(), cInfo);
                        System.out.println(this.coversInfo.size() + ":" + cInfo.getId() + ":" + cInfo.toString());
                    }
                } catch (Exception e) {
                    System.err.println("read learn data file failed file=" + fPath.toString());
                    System.err.println(this.getTrace(e));
                    continue;
                }
            }
        }

        // 文章属性信息在计算前加载
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // 获取已push文章存储路径
            String coversInfoPath = conf.get("push.video.coversinfo");
            // 读取已push视频专辑信息
            this.readCoversInfo(fs, coversInfoPath);
        }

        // 计算视频的均值向量
        private double[] statSimiVector(List<UserAction> hist) {
            double vec[] = new double[XnnModelItem.VECLEN];
            if (hist == null || hist.size() == 0) {
                return vec;
            }

            // 累加各个维度的权值
            int num = 0;
            for (int i = 0; i < hist.size(); i++) {
                XnnModelItem di = this.coversInfo.get(hist.get(i).getId());
                if (di != null) {
                    double tmp[] = di.getVec();
                    for (int j = 0; j < XnnModelItem.VECLEN; j++) {
                        vec[j] += tmp[j];
                    }
                    num++;
                }
            }

            // 求取最终向量
            if (num > 0) {
                for (int i = 0; i < XnnModelItem.VECLEN; i++) {
                    vec[i] /= num;
                }
            }
            return vec;
        }

        // 将权值数组组成字符串
        private String arrToString(double arr[]) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < XnnModelItem.VECLEN; i++) {
                sb.append(" ");
                sb.append(Double.parseDouble(String.format("%.4f", arr[i])));
            }
            return sb.substring(1);
        }

        public void reduce(Text uin, Iterable<UserAction> clickItems, Context context)
                throws IOException, InterruptedException {
            List<UserAction> allHist = new Vector<UserAction>();
            for (UserAction item : clickItems) {
                allHist.add(new UserAction(item));
            }

            // 存放点击实例的列表
            List<UserAction> clickHist = new Vector<UserAction>();
            // 存放没有点击实例的的列表
            List<UserAction> noClickHist = new Vector<UserAction>();

            // 对历史数据分类
            for (UserAction vc : allHist) {
                int click = vc.getVisitType();
                if (click == UserAction.CLICK || click == UserAction.VISIT) {
                    clickHist.add(vc);
                } else if (click == UserAction.NOCLICK) {
                    noClickHist.add(vc);
                }
            }

            // 计算没有用户点击视频的向量和用户不点击视频的向量
            double simiVec[] = this.statSimiVector(clickHist);
            double noSimiVec[] = this.statSimiVector(noClickHist);
            if (simiVec.length == XnnModelItem.VECLEN && noSimiVec.length == XnnModelItem.VECLEN) {
                String simiStr = arrToString(simiVec);
                String noSimiStr = arrToString(noSimiVec);
                context.write(uin, new Text(simiStr + " " + noSimiStr));
            } else {
                System.err.println("invalid vector");
            }
        }
    }
}