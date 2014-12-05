
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NewsDeepNNPredict {
    // 链接属性库
    private static Connection getToolCon(String encode) {
        Connection con = null;
        try {
            // 从mysql读取push视频的属性
            Class.forName("com.mysql.jdbc.Driver");
            // ?useUnicode=true&characterEncoding=UTF-8
            String url = "jdbc:mysql://10.198.30.118:3437/web_boss_tool?useUnicode=true&characterEncoding="+encode; //获取协议、IP、端口等信息
            String user = "web_boss_tool"; //获取数据库用户名
            String password = "3b989bb6e";//获取数据库用户密码
            con = DriverManager.getConnection(url, user, password); //创建Connection对象
        } catch (Exception e) {
            System.out.println("Conect mysql failed");
            System.err.println(e.getStackTrace());
        }
        return con;
    }

    // 获取专辑属性串
    private static String getNewsWords(String id) {
        String res = "";
        try {
            Connection con = getToolCon("UTF-8");
            Statement stmt = con.createStatement();

            // 读取数据
            String table = "t_tubd_article_info_app";
            String appId = id.substring(0,17) + "00";
            String cond = "appId='"+appId+"'";
            String sql = "select topic from " + table + " where " + cond;
            ResultSet rs = stmt.executeQuery(sql);
            if(rs.next()) {
                res = rs.getString(1);
            }
            System.out.println("XX"+res+"YY");
        } catch (Exception e) {
            System.out.println("Get news topic failed");
            System.err.println(e.getStackTrace());
        }
        return res;
    }

    // 计算专辑的向量
    private static String statNewsVec(String id, String keywords) {
        String res = "";
        // 组织查询关键字串
        String tmp = keywords.replaceAll(":[\\.0-9]*", "");
        String wordStr = "'" + tmp.replaceAll(",", "','") + "'";

        // 计算关键字对应的向量
        try {
            Connection con = getToolCon("UTF-8");
            Statement stmt = con.createStatement();
            if(con==null || stmt==null) {
                System.out.println("mysql connnection or stmt failed");
            }

            String sql = "select word, vec from tubd_mining.t_tubd_news_word_vec where word in ("+ wordStr +")";
            System.out.println("00 "+sql+" 11");
            double[] vecArr = new double[200];
            ResultSet rs = stmt.executeQuery(sql);
            int wordCnt = 0;
            while(rs.next()) {
                String word = rs.getString(1);
                String vec = rs.getString(2);
                //System.out.println("Word:"+word);
                //System.out.println("Vec:"+vec);
                String[] fields = vec.split(" ");
                for(int i=0; i<fields.length && i<200; i++) {
                    vecArr[i] += Double.parseDouble(fields[i]);
                }
                wordCnt++;
            }

            if(wordCnt>0) {
                System.out.println("has items");
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 200; i++) {
                    sb.append(vecArr[i] / wordCnt);
                    sb.append(" ");
                }
                res = sb.toString();
            } else {
                System.out.println("no items");
            }
            System.out.println("TotalVec:"+res);
        } catch (Exception e) {
            System.out.println("Stat vec failed");
            System.err.println(e.getStackTrace());
        }
        return id + "," + res;
    }

    private static String getTestId(String id) {
        String result = null;
        try{
            // 从mysql读取新闻的关键词
            String words = getNewsWords(id);

            // 根据关键词从mysql中读取向量
            result = statNewsVec(id, words);
        } catch(Exception e)  {
            System.out.println("Stat vec Exception:");
            System.out.println(e.toString());
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 7) {
            System.err.println("Usage: hadoop jar *.jar com.webdev.test.tubd.NewsDeepNNPredict <article_info> <hist_click> <sdate> <edate> <test_id> <suffix> <out_path>");
            System.exit(7);
        }

        // 参数获取
        String articleInfo = otherArgs[0];
        String clickHist = otherArgs[1];
        String sDate = otherArgs[2];
        String eDate = otherArgs[3];
        String testId = otherArgs[4];
        String suffix = otherArgs[5];
        String outPath = otherArgs[6];

        // 设置push所需信息
        //conf.set("mapred.max.map.failures.percent", "3");
        conf.set("push.news.path", articleInfo); // 历史push文章信息数据路径
        conf.set("push.news.sdate", sDate); // 开始时间
        conf.set("push.news.edate", eDate); // 结束时间
        conf.set("push.news.suffix", suffix);
        // 设置push预测的视频
        String newsStr = getTestId(testId);
        System.out.println(newsStr);
        conf.set("push.news.newspredict", newsStr);

        Job job = new Job(conf, "NewsDeepNNPredict");
        job.setJarByClass(NewsDeepNNPredict.class);
        job.setMapperClass(NewsDeepNNPredictMapper.class);
        job.setReducerClass(NewsDeepNNPredictReducer.class);
        job.setNumReduceTasks(400);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NewsUserClick.class);

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
            String tmpPath = clickHist + "/ds=" + formatter.format(cd1.getTime());
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

    static class NewsDeepNNPredictMapper
            extends Mapper<LongWritable, Text, Text, NewsUserClick> {
        private Pattern sufPattern = null;
        private Matcher sufMatcher = null;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            //String suffix = conf.get("push.news.suffix");
            //this.sufPattern = Pattern.compile("[13579]$");
            //this.sufPattern = Pattern.compile("^["+suffix+"]");
        }

        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            NewsUserClick newsUserClick = new NewsUserClick();
            try {  // 不能正常处理的记录丢弃之
                if (fields.length >= 3) {
                    // 判断后缀是否合法
                    //this.sufMatcher = this.sufPattern.matcher(fields[0]);
                    //if (this.sufMatcher.find()) {
                        newsUserClick.setUin(fields[0]);
                        newsUserClick.setAid(fields[1]);
                        newsUserClick.setClick(Integer.parseInt(fields[2]));
                        context.write(new Text(fields[0]), newsUserClick);
                    //}
                }
            } catch (Exception e) {
                System.err.println("failed Item:" + inValue);
                return;
            }
        }
    }

    public static class NewsDeepNNPredictReducer
            extends Reducer<Text, NewsUserClick, Text, Text> {
        // 已push文章信息
        private HashMap<String, NewsDeepNNInfo> pushNews = new HashMap<String, NewsDeepNNInfo>();

        private NewsDeepNNInfo testNews = new NewsDeepNNInfo();

        private Pattern sufPattern = null;

        private Matcher sufMatcher = null;

        private String getTrace(Throwable throwable) {
            StringWriter stringWriter = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }

        // 读取需要学习的文章
        private void readNewsInfo(FileSystem fs, String newsPath) {
            FileStatus[] stats = null;
            FSDataInputStream in = null;
            LineReader reader = null;
            System.out.println("Read Learn data");
            Path pDir = new Path(newsPath);
            try {
                if (!fs.exists(pDir)) {
                    System.out.println("Not exist file path: " + newsPath);  // debug
                    System.exit(19);
                }
                System.out.println("Exist file path: " + newsPath);  // debug
                stats = fs.listStatus(pDir);
            } catch (Exception e) {
                System.err.println("list learn data failed");
                System.err.println(this.getTrace(e));
                System.exit(21);
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
                try { // 读取学习数据
                    while (reader.readLine(learn) > 0) {
                        NewsDeepNNInfo aInfo = new NewsDeepNNInfo();
                        aInfo.setVec(learn.toString());
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

        // 文章属性信息在计算前加载
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // 获取已push文章存储路径
            String newsPath = conf.get("push.news.path");
            // 读取已push文章信息
            this.readNewsInfo(fs, newsPath);

            // 获取测试的新闻
            String newsStr = conf.get("push.news.newspredict");
            this.testNews.setVec(newsStr);

            //String suffix = conf.get("push.news.suffix");
            //this.sufPattern = Pattern.compile("^["+suffix+"]");
            this.sufPattern = Pattern.compile("^.{13}[123455789]");
        }

        //  计算文章和点击类或者非点击类的相似度
        private double similarProbability(NewsDeepNNInfo pNews, List<NewsUserClick> hist) {
            int histNum = hist.size();
            double similarity = 0.0;
            for (int i = 0; i < histNum; i++) {
                similarity += pNews.similarTo(this.pushNews.get(hist.get(i).getAid()));
            }
            return (histNum == 0) ? 0.0 : (similarity / histNum);
        }

        @Override
        public void reduce(Text uin, Iterable<NewsUserClick> clickItems, Context context)
                throws IOException, InterruptedException {
            // 读取用户行为数据
            List<NewsUserClick> allHist = new Vector<NewsUserClick>();
            for (NewsUserClick item : clickItems) {
                allHist.add(new NewsUserClick(item)); // 一定要重新生成一个copy，否则数据会有丢失
            }

            // 不满后缀的采用随机数据
            this.sufMatcher = this.sufPattern.matcher(uin.toString());
            if (!this.sufMatcher.find()) {
                if(Math.random()<0.3) {
                    context.write(uin, new Text("2"));
                }
                return;
            }

            // 存放点击实例的列表
            List<NewsUserClick> clickHist = new Vector<NewsUserClick>();
            // 存放没有点击实例的的列表
            List<NewsUserClick> noClickHist = new Vector<NewsUserClick>();

            // 对历史数据分类
            for (NewsUserClick uClick : allHist) {
                int click = uClick.getClick();
                if (click == UserClickType.CLICK) {
                    clickHist.add(uClick);
                } else if (click == UserClickType.NOCLICK) {
                    noClickHist.add(uClick);
                }
            }

            // 计算预测数据与两类数据的相似度
            double clikRate = this.similarProbability(this.testNews, clickHist);
            double noClikRate = this.similarProbability(this.testNews, noClickHist);

            // 做出预测
            double probability = (clikRate==0 || noClikRate==0) ? 0 : clikRate/noClikRate;
            int predict = (probability > 0.1) ? UserClickType.CLICK : UserClickType.NOCLICK;
            Text result = new Text(predict + "\t" + probability + "\t" + clikRate + "\t" + noClikRate);
            context.write(uin, result);
        }
    }
}