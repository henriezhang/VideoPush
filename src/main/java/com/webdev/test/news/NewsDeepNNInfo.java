
package com.webdev.test.news;

/**
 * Created by henriezhang on 2014/8/12.
 */
public class NewsDeepNNInfo
{
    // App文章ID
    private String aid = "";

    private String rawStr = "";

    // 专辑视频向量的长度
    private int vecLen = 200;

    private double[] vec = new double[200];

    public NewsDeepNNInfo() {

    }

    // 根据push预测数据建立
    public void setVec(String str) {
        this.rawStr = str;
        // 设置专辑ID
        String[] item = str.split("(\u0001)|,");
        this.aid = item[0];
        // 设置专辑对应的向量
        String[] fields = item[1].split(" ");
        for(int i=0; i<fields.length && i<vecLen; i++) {
            vec[i] = Double.parseDouble(fields[i]);
        }
    }

    public String getAid() {
        return aid;
    }

    public double[] getVec() {
        return vec;
    }

    private double cosinSimi(double[] vec1, double[] vec2) {
        // 入参判断
        if(vec1==null || vec2==null || vec1.length!=vecLen || vec2.length!=vecLen) {
            return 0.0;
        }

        double muldot = 0.0, square1 = 0.0, square2 = 0.0;
        for(int i=0; i<vecLen; i++) {
            muldot += vec1[i] * vec2[i];
            square1 += vec1[i] * vec1[i];
            square2 += vec2[i] * vec2[i];
        }
        double denominator = (Math.sqrt(square1) * Math.sqrt(square2));

        if (denominator < muldot) {
            denominator = muldot;
        }

        // correct for zero-vector corner case
        if (denominator == 0 || muldot == 0)
        {
            return 0.0;
        }

        return muldot / denominator;
    }

    public double similarTo(NewsDeepNNInfo aInfo) {
        if(aInfo == null) {
            return 0.0;
        }
        return this.cosinSimi(this.vec, aInfo.getVec());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(this.aid+"\t");
        sb.append(this.rawStr);
        return sb.toString();
    }
}