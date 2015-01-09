package com.webdev.entity;

/**
 * Created by henriezhang on 2014/11/6.
 */
public class PnnModelUser {
    private String uin;

    private double[] simiVec = new double[DnnModelItem.VECLEN];

    private double[] noSimiVec = new double[DnnModelItem.VECLEN];

    private double clickRate = 0.0;

    private double noClickRate = 0.0;

    public double getClickRate() {
        return clickRate;
    }

    public double getNoClickRate() {
        return noClickRate;
    }

    // 初始化用户模型数据
    public boolean setVec(String uin, String str) {
        this.uin = uin;

        // 设置专辑对应的向量
        String[] fields = str.split(" ");
        for (int i = 0; i < DnnModelItem.VECLEN && i + DnnModelItem.VECLEN < fields.length; i++) {
            this.simiVec[i] = Double.parseDouble(fields[i]);
            this.noSimiVec[i] = Double.parseDouble(fields[i + DnnModelItem.VECLEN]);
        }
        return true;
    }

    // 返回0~1的权值
    private double cosinSimi(double[] vec1, double[] vec2) {
        // 入参判断
        if (vec1 == null || vec2 == null || vec1.length != DnnModelItem.VECLEN || vec2.length != DnnModelItem.VECLEN) {
            return 0.0;
        }

        double muldot = 0.0, square1 = 0.0, square2 = 0.0;
        for (int i = 0; i < DnnModelItem.VECLEN; i++) {
            muldot += vec1[i] * vec2[i];
            square1 += vec1[i] * vec1[i];
            square2 += vec2[i] * vec2[i];
        }
        double denominator = (Math.sqrt(square1) * Math.sqrt(square2));

        if (denominator < muldot) {
            denominator = muldot;
        }

        // 计算余弦值
        double cos = 0.0;
        if (denominator != 0 && muldot != 0) {
            cos = muldot / denominator;
        }
        return cos;
    }

    public double similarTo(DnnModelItem item, double gradient) {
        if (item == null) {
            return 0.0;
        }
        clickRate = this.cosinSimi(this.simiVec, item.getVec());
        noClickRate = this.cosinSimi(this.noSimiVec, item.getVec());

        // //距离计算
        // //斜率和分母依次对应： <0.8, 1.28>; <0.9, 1.35>; <1, 1.41>； <1.1, 1.49>； <1.2, 1.56>； <1.3, 1.64>； <1.4, 1.72>；
        // //<2, 2.4>; <3, 3.16>；<1000, 1000>
        //double gradient = 1, denominator = 1.41;
        //double diff = gradient*clickRate - noClickRate;
        // //stratage 1
        //double probability = diff / denominator;

        // stratage 1
        // double probability = (gradient*clickRate - noClickRate) / Math.sqrt(Math.pow(1, 2) + Math.pow(gradient, 2));
        double probability = (gradient*clickRate - noClickRate) / Math.sqrt(1.0 + Math.pow(gradient, 2));

        return probability;
    }
}
