package com.webdev.test.tubd;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by henriezhang on 2014/4/5.
 */
public class ArticleInfo implements Writable {
    //App文章ID
    private String aid = "";
    //文章标题
    private String title = "";
    // 文章push时间
    private String pushTime = "";
    // 转化为1周序列中第n个小时
    private int pushTimeExt = -1;
    // 文章类型
    private String infoType = "";
    // 文章标志
    private String flag = "";
    // 文章关键字
    private String topic = "";
    private HashMap<String, Double> topicExt = new HashMap<String, Double>();
    //文章标题关键字
    private String titleTopic = "";
    private HashMap<String, Double> titleTopicExt = new HashMap<String, Double>();

    public ArticleInfo() {
        super();
    }

    public ArticleInfo(ArticleInfo aItem) {
        this.setAid(aItem.aid);
        this.setTitle(aItem.title);
        this.setPushTime(aItem.pushTime);
        this.setInfoType(aItem.infoType);
        this.setFlag(aItem.flag);
        this.setTopic(aItem.topic);
        this.setTitleTopic(aItem.titleTopic);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.aid);
        out.writeUTF(this.title);
        out.writeUTF(this.pushTime);
        out.writeUTF(this.infoType);
        out.writeUTF(this.flag);
        out.writeUTF(this.topic);
        out.writeUTF(this.titleTopic);
    }

    public void readFields(DataInput in) throws IOException {
        this.aid = in.readUTF();
        this.title = in.readUTF();
        this.pushTime = in.readUTF();
        this.infoType = in.readUTF();
        this.flag = in.readUTF();
        this.topic = in.readUTF();
        this.titleTopic = in.readUTF();
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setPushTime(String pushTime) {
        this.pushTime = pushTime;
        // 计算push时间属于一天的第几个小时
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date dt = formatter.parse(this.pushTime);
            Calendar cd = Calendar.getInstance();
            cd.setTime(dt);
            this.pushTimeExt = cd.DAY_OF_WEEK * 24 + cd.HOUR_OF_DAY;
        } catch (Exception e) {

        }
    }

    public int getPushTimeExt() {
        return pushTimeExt;
    }

    public HashMap<String, Double> getTopicExt() {
        return topicExt;
    }

    public HashMap<String, Double> getTitleTopicExt() {
        return titleTopicExt;
    }

    public void setInfoType(String infoType) {
        this.infoType = infoType;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void setTopic(String topic) {
        this.topic = topic;
        // 将文章关键字已关键字为key，value为权重存入hashmap
        String[] keywords = this.topic.split(",", 20);
        for (int i = 0; i < keywords.length; i++) {
            String[] item = keywords[i].split(":", 2);
            if (item.length == 2) {
                this.topicExt.put(item[0], Double.parseDouble(item[1]));
            }
        }
    }

    public void setTitleTopic(String titleTopic) {
        this.titleTopic = titleTopic;
        // 将标题关键字已关键字为key，value为权重存入hashmap
        String[] keywords = this.titleTopic.split(",", 20);
        for (int i = 0; i < keywords.length; i++) {
            this.titleTopicExt.put(keywords[i], 1.0);
            /*
            String[] item = this.topic.split(":", 2);
            if (item.length >= 1) {
                this.titleTopicExt.put(item[0], 1.0);
            }
            */
        }
    }

    public String getAid() {
        return aid;
    }

    public String getTitle() {
        return title;
    }

    public String getPushTime() {
        return pushTime;
    }

    public String getInfoType() {
        return infoType;
    }

    public String getFlag() {
        return flag;
    }

    public String getTopic() {
        return topic;
    }

    public String getTitleTopic() {
        return titleTopic;
    }

    private double cacuSimilarity(HashMap<String, Double> h1, HashMap<String, Double> h2) {
        if(h1==null || h2==null || h1.size()==0 || h2.size()==0) {
            return 0.0;
        }
        HashSet<String> words = new HashSet<String>();
        words.addAll(h1.keySet());
        words.addAll(h2.keySet());

        double muldot = 0.0;
        double square1 = 0.0;
        double square2 = 0.0;
        for (String word : words) {
            Double W1 = h1.get(word);
            Double W2 = h2.get(word);
            double w1 = (W1==null) ? 0 : W1.doubleValue();
            double w2 = (W2==null) ? 0 : W2.doubleValue();
            muldot += w1 * w2;
            square1 += w1 * w1;
            square2 += w2 * w2;
        }
        return muldot / (Math.sqrt(square1+0.0001) * Math.sqrt(square2+0.0001));
    }

    public double similarTo(ArticleInfo aInfo) {
        /*if(Math.round(Math.random()*10000)==10) {
            System.out.println(this.toString());
        }*/
        if(aInfo == null) {
            return 0.0;
        }
        // type相似度
        //double typeSimi = (this.infoType == aInfo.getInfoType()) ? 1 : 0;
        // flag相似度
        //double flagSimi = (this.flag == aInfo.getFlag()) ? 1 : 0;
        // 时间相似度
        //double hourSimi = (this.pushTimeExt == aInfo.getPushTimeExt()) ? 1 : 0;
        //double hourSimi = (Math.abs(this.pushTimeExt-aInfo.getPushTimeExt())<=1) ? 1 : 0;
        // 标题关键词相似度
        //double titleTopicSimi = this.cacuSimilarity(this.titleTopicExt, aInfo.getTitleTopicExt());
        // 文章关键词相似度
        double topicSimi = this.cacuSimilarity(this.topicExt, aInfo.getTopicExt());
        //double totalSimi = typeSimi * 0.05 + flagSimi * 0.05 + hourSimi * 0.2;
        //double totalSimi = titleTopicSimi*0.8+topicSimi*0.2;
        double totalSimi = topicSimi;
        //double totalSimi = typeSimi*0.05+flagSimi*0.05+hourSimi*0.1+titleTopicSimi*0.6+topicSimi*0.2;
        return totalSimi;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(this.getAid()+"\t");
        sb.append(this.getTitle()+"\t");
        sb.append(this.getTitleTopic()+"\t");
        sb.append(this.getPushTime()+"\t");
        sb.append(this.getInfoType()+"\t");
        sb.append(this.getTopic()+"\t");
        sb.append(this.getTopicExt().size()+"\t");
        sb.append(this.getTitleTopicExt().size()+"\n");
        return sb.toString();
    }
}
