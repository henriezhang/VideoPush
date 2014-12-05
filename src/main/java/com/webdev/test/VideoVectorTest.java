package com.webdev.test;

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by henriezhang on 2014/11/4.
 */
public class VideoVectorTest {
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
            // 设置编码
            //String enCode = "set names utf8";
            //boolean ret = stmt.execute(enCode);
            //System.out.println("set names ret="+ret);

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
}
