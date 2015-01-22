package com.webdev.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by henriezhang on 2014/4/5.
 */

public class UserAction implements Writable {
    public final static int VISIT = 2; // 表示有点击
    public final static int CLICK = 1; // 表示有点击
    public final static int NOCLICK = 0; // 表示没有点击

    private String uin = ""; // 用户ID
    private String id = "";  // 视频ID或者专辑ID
    private int visitType = 0;
    private int cnt = 0; // 对应访问行为操作次数
    private String vec = "";

    public UserAction() {
        super();
    }

    public UserAction(UserAction v) {
        this.uin = v.uin;
        this.id = v.id;
        this.visitType = v.visitType;
        this.cnt = v.cnt;
        this.vec = v.vec;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uin);
        out.writeUTF(this.id);
        out.writeInt(this.visitType);
        out.writeInt(this.cnt);
        out.writeUTF(this.vec);
    }

    public void readFields(DataInput in) throws IOException {
        this.uin = in.readUTF();
        this.id = in.readUTF();
        this.visitType = in.readInt();
        this.cnt = in.readInt();
        this.vec = in.readUTF();
    }

    public String getUin() {
        return uin;
    }

    public String getId() {
        return id;
    }

    public int getVisitType() {
        return visitType;
    }

    public int getCnt() {
        return cnt;
    }

    public String getVec() {
        return vec;
    }

    public void setUin(String uin) {
        this.uin = uin;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setVisitType(String visitType) {
        int vType = 0;
        try{
            vType = Integer.parseInt(visitType);
        } catch (Exception e){ }
        setVisitType(vType);
    }

    public void setVisitType(int vType) {
        if(vType==UserAction.CLICK || vType==UserAction.VISIT) {
            this.visitType = vType;
        } else {
            this.visitType = UserAction.NOCLICK;
        }
    }

    public void setCnt(String cnt) {
        try{
            this.cnt = Integer.parseInt(cnt);
        } catch (Exception e){
            this.cnt = 0;
        }
    }

    public void setVec(String vec) {
        this.vec = vec;
    }

    public String toString() {
        return this.uin + "\t" + this.id + "\t" + this.visitType;
    }
}
