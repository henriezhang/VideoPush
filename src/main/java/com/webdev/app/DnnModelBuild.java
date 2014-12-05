package com.webdev.app;

import com.webdev.entity.DnnModelItem;
import com.webdev.entity.VideoClick;
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
public class DnnModelBuild {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 6) {
            System.err.println("Usage: hadoop jar VideoPush.jar com.webdev.app.DnnModelBuild <cover_info> <hist_click> <sdate> <edate> <tfrom> <out_path>");
            System.exit(6);
        }

        // 参数获取
        String coversInfo = otherArgs[0];
        String clickHist = otherArgs[1];
        String sDate = otherArgs[2];
        String eDate = otherArgs[3];
        String tfrom = otherArgs[4];
        String outPath = otherArgs[5];

        // 设置push所需信息
        conf.set("push.video.coversinfo", coversInfo); // 历史push专辑信息数据路径
        conf.set("push.video.sdate", sDate); // 开始时间
        conf.set("push.video.edate", eDate); // 结束时间

        Job job = new Job(conf, "DnnModelBuild");
        job.setJarByClass(DnnModelBuild.class);
        job.setMapperClass(DnnModelBuildMapper.class);
        job.setReducerClass(DnnModelBuildReducer.class);
        job.setNumReduceTasks(400);

        // the map output is Text, VideoClick
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VideoClick.class);

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
            String tmpPath = clickHist + "/ds=" + formatter.format(cd1.getTime()) + "/tfrom=" + tfrom;
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
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class DnnModelBuildMapper
            extends Mapper<LongWritable, Text, Text, VideoClick> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t"); // \u0001
            // guid 合法性判断
            if (fields[0].length() != 32) {
                return;
            }

            if (fields.length >= 3) {
                VideoClick vc = new VideoClick();
                vc.setId(fields[1]);
                vc.setClick(fields[2]);
                context.write(new Text(fields[0]), vc);
            }
        }
    }

    public static class DnnModelBuildReducer
            extends Reducer<Text, VideoClick, Text, Text> {
        // 已push文章信息
        private HashMap<String, DnnModelItem> coversInfo = new HashMap<String, DnnModelItem>();

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
                        DnnModelItem cInfo = new DnnModelItem();
                        cInfo.setVecBuild(test.toString());
                        this.coversInfo.put(cInfo.getId(), cInfo);
                        //System.out.println( this.coversInfo.size() + ":" + cInfo.getId() + ":" + cInfo.toString());
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
        private double[] statSimiVector(List<VideoClick> hist) {
            double vec[] = new double[DnnModelItem.VECLEN];
            if (hist == null || hist.size() == 0) {
                return vec;
            }

            // 累加各个维度的权值
            int num = 0;
            for (int i = 0; i < hist.size(); i++) {
                DnnModelItem di = this.coversInfo.get(hist.get(i).getId());
                if (di != null) {
                    double tmp[] = di.getVec();
                    for (int j = 0; j < DnnModelItem.VECLEN; j++) {
                        vec[j] += tmp[j];
                    }
                    num++;
                }
            }

            // 求取最终向量
            if (num > 0) {
                for (int i = 0; i < DnnModelItem.VECLEN; i++) {
                    vec[i] /= num;
                }
            }
            return vec;
        }

        // 将权值数组组成字符串
        private String arrToString(double arr[]) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < DnnModelItem.VECLEN; i++) {
                sb.append(" ");
                //sb.append(String.format("%.4f", arr[i]));
                sb.append(Double.parseDouble(String.format("%.4f", arr[i])));
            }
            return sb.substring(1);
        }

        @Override
        public void reduce(Text uin, Iterable<VideoClick> clickItems, Context context)
                throws IOException, InterruptedException {
            List<VideoClick> allHist = new Vector<VideoClick>();
            for (VideoClick item : clickItems) {
                allHist.add(new VideoClick(item)); // 一定要重新生成一个copy，否则数据会有丢失
            }

            // 存放点击实例的列表
            List<VideoClick> clickHist = new Vector<VideoClick>();
            // 存放没有点击实例的的列表
            List<VideoClick> noClickHist = new Vector<VideoClick>();

            // 对历史数据分类
            for (VideoClick vc : allHist) {
                int click = vc.getClick();
                if (click == VideoClick.CLICK) {
                    clickHist.add(vc);
                } else if (click == VideoClick.NOCLICK) {
                    noClickHist.add(vc);
                }
            }

            // 计算没有用户点击视频的向量和用户不点击视频的向量
            double simiVec[] = this.statSimiVector(clickHist);
            double noSimiVec[] = this.statSimiVector(noClickHist);
            if (simiVec.length == DnnModelItem.VECLEN && noSimiVec.length == DnnModelItem.VECLEN) {
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