
package com.webdev.test.news;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.*;

/**
 * Created by henriezhang on 2014/4/5.
 */
public class DeepArticleInfo implements Writable {
    // App文章ID
    private String aid = "";

    // 文章关键词
    private HashMap<String, Vector<Double>> keywords = new HashMap<String, Vector<Double>>();

    // 打印跟踪信息
    private String getTrace(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    // 文章关键词deep向量
    private Vector<Double> wordVector = new Vector<Double>();

    public DeepArticleInfo() {
        super();
    }

    public DeepArticleInfo(DeepArticleInfo aItem) {
        this.setAid(aItem.aid);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.aid);
    }

    public void readFields(DataInput in) throws IOException {
        this.aid = in.readUTF();
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public String getAid() {
        return aid;
    }

    // 计算文章向量关键词均值
    private void statAvgNews() {
        int keySum = 0;
        double vec[] = new double[200];
        Iterator it = this.keywords.entrySet().iterator();
        while (it.hasNext()) {
            keySum++;
            Map.Entry entry = (Map.Entry) it.next();
            Vector<Double> v = (Vector<Double>) entry.getValue();
            for (int j = 0; j < v.size(); j++) {
                vec[j] += v.get(j);
            }
        }

        for (int i = 0; i < vec.length; i++) {
            this.wordVector.add(vec[i] / keySum);
        }
    }

    // 初始化文章向量
    public boolean setNews(String value) {
        String[] item = value.split("\t", 2);
        if (item.length < 2) {
            return false;
        }
        this.aid = item[0]; // 解析文章ID
        String words[] = item[1].split(",", 20);
        for (int i = 0; i < words.length; i++) {
            String word_vec = words[i];   // 解析出关键词向量
            String kp[] = word_vec.split(":", 2);
            String k = kp[0]; // 解析出关键词
            String vec[] = kp[1].split(" ", 200);
            Vector<Double> v = new Vector<Double>();
            for (int j = 0; j < vec.length; j++) {
                double mv = 0.0;
                try {
                    mv = Double.parseDouble(vec[j]);
                } catch (Exception e) {
                    System.err.println(this.getTrace(e));
                }
                v.add(j, mv);
            }
            this.keywords.put(k, v);
        }
        statAvgNews();
        return true;
    }

    public void setKeywords(HashMap<String, Vector<Double>> keywords) {
        this.keywords = keywords;
    }

    public HashMap<String, Vector<Double>> getKeywords() {
        return this.keywords;
    }

    public void setWordVector(Vector<Double> wordVector) {
        this.wordVector = wordVector;
    }

    public Vector<Double> getWordVector() {
        return this.wordVector;
    }

    private double cacuSimilarity(Vector<Double> v1, Vector<Double> v2) {
        if (v1 == null || v2 == null || v1.size() == 0 || v2.size() == 0) {
            return 0.0;
        }

        double muldot = 0.0;
        double square1 = 0.0;
        double square2 = 0.0;
        for (int i = 0, len = Math.min(Math.min(v1.size(), v2.size()), 200); i < len; i++) {
            double w1 = v1.get(i);
            double w2 = v2.get(i);
            muldot += w1 * w2;
            square1 += w1 * w1;
            square2 += w2 * w2;
        }
        double simi = 0.0;
        try {
            simi = muldot / (Math.sqrt(square1) * Math.sqrt(square2));
        } catch (Exception e) {
        }
        return simi;
    }

    public double similarTo(DeepArticleInfo aInfo) {
        if (aInfo == null) {
            return 0.0;
        }
        return this.cacuSimilarity(this.wordVector, aInfo.getWordVector());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(this.getAid() + "\t");
        return sb.toString();
    }
}
