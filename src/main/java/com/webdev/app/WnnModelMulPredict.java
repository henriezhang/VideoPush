package com.webdev.app;

import com.webdev.entity.DnnModelItem;
import com.webdev.entity.DnnModelUser;
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
import java.util.List;
import java.util.Vector;

/**
 * Created by henriezhang on 2014/11/5.
 */
public class WnnModelMulPredict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", "gboss");
        conf.set("mapred.queue.name", "gboss");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: hadoop jar videopush.jar com.webdev.app.DnnModelMulPredict <user_model> <out_path> <predict_info>");
            System.exit(3);
        }
        String userModel = otherArgs[0];
        String outPath = otherArgs[1];
        String predictInfo = otherArgs[2];

        conf.set("push.video.predictinfo", predictInfo);
        Job job = new Job(conf, "VideoPush");
        job.setJarByClass(WnnModelMulPredict.class);
        job.setMapperClass(WnnModelMulPredictMapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(0);
        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定输入路径
        Path uPath = new Path(userModel);
        FileInputFormat.addInputPath(job, uPath);

        // 指定输出文件路径
        Path oPath = new Path(outPath);
        // 如果输出路径已经存在则清除之
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(oPath)) {
            fs.deleteOnExit(oPath);
        }
        FileOutputFormat.setOutputPath(job, oPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WnnModelMulPredictMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private List<DnnModelItem> ids = new Vector<DnnModelItem>();

        private boolean initPredictIds(String info) {
            String[] items = info.split(":");
            for (int i = 0; i < items.length; i++) {
                DnnModelItem di = new DnnModelItem();
                if (di.setVecPredict(items[i])) {
                    System.err.println(di.getId() + ":" + di.toString());
                    this.ids.add(di);
                }
            }
            return this.ids.size() > 0;
        }

        // 初始化数据
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // 读取测试视频专辑信息
            String predictinfo = conf.get("push.video.predictinfo");
            //System.err.println(predictinfo);

            if (!this.initPredictIds(predictinfo)) {
                System.err.println("init predict info failed");
                System.exit(10);
            }
        }

        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            // 判断字段数个数是否合法
            if (fields.length < 2 || fields[0].length() != 32) {
                System.out.println(inValue);
                return;
            }

            // 获取最大取值的视频和权重
            StringBuilder sb = new StringBuilder();
            DnnModelItem maxItem = null;
            double maxRate = 0.0;
            double maxClickRate = 0.0;
            double maxNoClickRate = 0.0;
            DnnModelUser u = new DnnModelUser();
            if (u.setVec(fields[0], fields[1])) {
                for (int i = 0; i < this.ids.size(); i++) {
                    DnnModelItem item = this.ids.get(i);
                    double tmp = u.similarTo(item);
                    sb.append(item.getId() + " " + tmp + ":");
                    if (tmp >= maxRate) {
                        maxRate = tmp;
                        maxItem = item;
                        maxClickRate = u.getClickRate();
                        maxNoClickRate = u.getNoClickRate();
                    }
                }
                //context.write(new Text("size"), new Text(""+this.ids.size()+""+maxItem.toString()));
            }

            // 输出结果
            if (maxItem != null) {
            context.write(new Text(fields[0]),
                    new Text(maxItem.getId() + "\t" + maxItem.getPid() + "\t" + maxRate + "\t"
                            + maxClickRate + "\t" + maxNoClickRate + "\t" + sb.toString()));
            }
        }
    }
}

