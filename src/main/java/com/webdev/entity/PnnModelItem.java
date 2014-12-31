package com.webdev.entity;

/**
 * Created by henriezhang on 2014/11/5.
 */
public class PnnModelItem {
    // 专辑视频向量的长度
    public final static int VECLEN = 200;
    private double[] vec = new double[PnnModelItem.VECLEN];

    // 专辑ID
    private String id = "";
    private String rawStr = "";
    private String pid ="";

    public PnnModelItem() {

    }

    public String getPid() {
        return pid;
    }

    public String getId() {
        return id;
    }

    public double[] getVec() {
        return vec;
    }

    // 建模时历史Push视频向量初始化
    public boolean setVecBuild(String str) {
        this.rawStr = str;
        // 设置专辑ID
        String[] item = str.split("(\u0001)|,");
        this.id = item[0];
        // 设置专辑对应的向量
        String[] fields = item[1].split(" ");
        for (int i = 0; i < fields.length && i < PnnModelItem.VECLEN; i++) {
            vec[i] = Double.parseDouble(fields[i]);
        }
        return true;
    }

    // 预测时预测视频向量初始化
    public boolean setVecPredict(String str) {
        this.rawStr = str;
        // 设置专辑ID
        String[] item = str.split("\\+");
        this.id = item[0];
        this.pid = item[1];
        // 设置专辑对应的向量
        String[] fields = item[2].split(",");
        for (int i = 0; i < fields.length && i < PnnModelItem.VECLEN; i++) {
            vec[i] = Double.parseDouble(fields[i]);
        }
        return true;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(this.id + "\t");
        sb.append(this.rawStr);
        return sb.toString();
    }
}