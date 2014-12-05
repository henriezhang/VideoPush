/*
Func: bayes classfier
Ahther: henriezhang
Date: 2014-07-23
 */
package com.webdev.app;

import com.webdev.entity.DeepNNCover;
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
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DeepNNPredict {
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
    private static String getCoverRaw(String id) {
        String res = "";
        try {
            Connection con = getToolCon("UTF-8");
            Statement stmt = con.createStatement();

            // 读取数据
            String table = "t_tubd_video_cid_info";
            String cond = "cid='"+id+"'";
            if(id.length()==11) {
                table = "t_tubd_video_vid_info";
                cond = "vid='"+id+"'";
            }
            String sql = "select title,type_name,subtype_name,area_name,director," +
                    "leading_actor,main_aspect,plot_brief,visual_brief,viewing_experience," +
                    "awards,user_reviews,famous_actor,guests,variety_tags," +
                    "aspect_tag,big_events,cartoon_aspect,production_company,cartoon_director," +
                    "original_author,brief,description " +
                    "from " + table + " where " + cond;
            StringBuilder sb = new StringBuilder();
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("Col count:"+rs.getMetaData().getColumnCount());
            if(rs.next()) {
                for(int i=1; i<=rs.getMetaData().getColumnCount(); i++) {
                    sb.append(rs.getString(i));
                }
            }
            res = sb.toString();
            System.out.println("XX"+res+"YY");
        } catch (Exception e) {
            System.out.println("Get cover raw failed");
            System.err.println(e.getStackTrace());
        }
        return res;
    }

    // 计算专辑的关键词
    private static JsonNode statCoverWords(String rawStr){
        JsonNode keywords = null;
        try {
            // POST的URL
            HttpPost httppost = new HttpPost("http://10.129.138.54:8081");
            // 建立HttpPost对象
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            // 建立一个NameValuePair数组，用于存储欲传送的参数
            params.add(new BasicNameValuePair("op", "keyword"));
            params.add(new BasicNameValuePair("weight", "false"));
            params.add(new BasicNameValuePair("count", "20"));
            params.add(new BasicNameValuePair("title", ""));
            params.add(new BasicNameValuePair("content", rawStr));
            // 设置编码
            httppost.setEntity(new UrlEncodedFormEntity(params, HTTP.UTF_8));
            // 发送Post,并返回一个HttpResponse对象
            HttpResponse response = new DefaultHttpClient().execute(httppost);
            // 如果状态码为200,就是正常返回
            if(response.getStatusLine().getStatusCode()==200) {
                String result = EntityUtils.toString(response.getEntity());
                // 解析json数据
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(result);
                keywords = rootNode.path("keyword");
            }
        } catch (Exception e) {
            System.out.println("Get cover words failed");
            System.out.println(e.getStackTrace());
        }
        return keywords;
    }

    // 计算专辑的向量
    private static String statCoverVec(String id, JsonNode keywords) {
        String res = "";
        // 拼接关键字
        StringBuilder sbk = new StringBuilder();
        for(int i=0; i<keywords.size() && i<20; i++) {
            sbk.append(",'");
            sbk.append(keywords.get(i).toString().replace("\"",""));
            sbk.append("'");
        }
        String wordStr = sbk.substring(1);

        // 计算关键字对应的向量
        try {
            Connection con = getToolCon("UTF-8");
            Statement stmt = con.createStatement();
            if(con==null || stmt==null) {
                System.out.println("mysql connnection or stmt failed");
            }
            // 设置编码
            //String enCode = "set names latin1";
            //boolean ret = stmt.execute(enCode);
            //System.out.println("set names ret="+ret);
            String sql = "select word, vec from tubd_mining.t_tubd_video_word_vec where word in ("+ wordStr +")";
            System.out.println("00 "+sql+" 11");
            double[] vecArr = new double[200];
            ResultSet rs = stmt.executeQuery(sql);
            int wordCnt = 0;
            while(rs.next()) {
                String word = rs.getString(1);
                String vec = rs.getString(2);
                System.out.println("Word:"+word);
                System.out.println("Vec:"+vec);
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
            // 从mysql读取push视频的属性
            String rawStr = getCoverRaw(id);
            // 将视频属性提取关键词
            JsonNode words = statCoverWords(rawStr);
            // 根据关键词从mysql中读取向量
            result = statCoverVec(id, words);
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
            System.err.println("Usage: hadoop jar BossBayesPredict.jar com.webdev.app.BayesTagsPredict <cover_info> <hist_click> <sdate> <edate> <test_id> <prefix> <out_path>");
            System.exit(7);
        }

        // 参数获取
        String coversInfo = otherArgs[0];
        String clickHist = otherArgs[1];
        String sDate = otherArgs[2];
        String eDate = otherArgs[3];
        String testId = otherArgs[4];
        String prefix = otherArgs[5];
        String outPath = otherArgs[6];

        // 设置push所需信息
        //conf.set("mapred.max.map.failures.percent", "3");
        conf.set("push.video.coversinfo", coversInfo); // 历史push专辑信息数据路径
        conf.set("push.video.sdate", sDate); // 开始时间
        conf.set("push.video.edate", eDate); // 结束时间
        conf.set("push.video.prefix", prefix);
        // 设置push预测的视频
        String coverStr = getTestId(testId);
        System.out.println(coverStr);
        conf.set("push.video.coverpredict", coverStr);

        Job job = new Job(conf, "VideoDeepNN");
        job.setJarByClass(DeepNNPredict.class);
        job.setMapperClass(DeepNNPredictMapper.class);
        job.setReducerClass(DeepNNPredictReducer.class);
        job.setNumReduceTasks(300);

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
        for (int i = 1; Integer.parseInt(formatter.format(cd1.getTime())) <= endDs && i < 360; i++) {
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

    public static class DeepNNPredictMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private Pattern prePattern = null;
        private Matcher preMatcher = null;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            String prefix = conf.get("push.video.prefix");
            this.prePattern = Pattern.compile("^["+prefix+"]");
        }

        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\u0001");
            // 过滤符合前缀条件的用户
            this.preMatcher = this.prePattern.matcher(fields[0]);
            if(!this.preMatcher.find()) {
                return;
            }

            if(fields.length>=2) {
                context.write(new Text(fields[0]), new Text(fields[1]));  // out put guid and cid
            }
        }
    }

    public static class DeepNNPredictReducer
            extends Reducer<Text, Text, Text, Text> {
        // 已push文章信息
        private HashMap<String, DeepNNCover> coversInfo = new HashMap<String, DeepNNCover>();

        private DeepNNCover predictCover = new DeepNNCover();

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
                try { // 读取测试数据
                    while (reader.readLine(test) > 0) {
                        DeepNNCover cInfo = new DeepNNCover();
                        cInfo.setVec(test.toString());
                        this.coversInfo.put(cInfo.getCid(), cInfo);
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

            // 解析预测视频专辑信息
            String coverStr = conf.get("push.video.coverpredict");
            this.predictCover.setVec(coverStr);
        }

        @Override
        public void reduce(Text uin, Iterable<Text> clickItems, Context context)
                throws IOException, InterruptedException {
            // 读取用户行为数据
            Set<String> clickSet = new HashSet<String>();
            for (Text item : clickItems) {
                clickSet.add(item.toString()); // 一定要重新生成一个copy，否则数据会有丢失
            }

            // 计算点击概率
            Iterator<String> iterator = clickSet.iterator();
            double simi = 0.0;
            while(iterator.hasNext()){
                String key = iterator.next();
                DeepNNCover val = this.coversInfo.get(key);
                simi += this.predictCover.similarTo(val);
            }
            double clickRate = (simi==0 || clickSet.size()==0) ? 0.0 : simi/clickSet.size();

            // 当前用户跟未点击过历史数据相似的概率
            double noSimi = 0.0; int noSimiLen = 0;
            Iterator iter = this.coversInfo.entrySet().iterator();
            int standLen = clickSet.size() * 2; // 计算预测数据的样例数
            while (iter.hasNext() && noSimiLen<standLen) {
                Map.Entry entry = (Map.Entry) iter.next();
                String key = (String)entry.getKey();
                if(!clickSet.contains(key)){
                    DeepNNCover val = (DeepNNCover)entry.getValue();
                    noSimi += this.predictCover.similarTo(val);
                    noSimiLen++;
                }
            }
            double noClickRate = (noSimi==0 || noSimiLen==0) ? 0.0 : noSimi/noSimiLen;

            // 权值计算
            //double probability = (clickRate * clickRate) / noClikRate;

            /*
            double probability = 0.0;
            if(noSimi==0) {
                probability = (clikRate>0) ? 1.0 : 0.0;
            } else {
                probability = (clikRate*clikRate*10) / Math.abs(noClikRate);
            }*/

            // 纯比值计算4.0
            /*double probability = 0.0;
            if(noSimi==0) {
                probability = (clickRate>0) ? 1.0 : 0.0;
            } else {
                probability = clickRate / noClickRate;
            }*/

            // 2.0 点到线分割线的法线距离
            double probability = Math.abs(clickRate - noClickRate) / Math.sqrt(Math.sqrt(clickRate) + Math.sqrt(noClickRate));
            if(clickRate - noClickRate < 0) { // 如果不在所需平面则使用负距离
                probability *= -1;
            }

            // 做出判断
            int predict = (probability > 0.1) ? 1 : 0;
            Text result = new Text(predict + "\t" + probability + "\t" + clickRate + "\t" + noClickRate);
            context.write(uin, result);
        }
    }
}