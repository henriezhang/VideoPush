package com.webdev.test.news;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by henriezhang on 2014/4/5.
 */

public class UserClick implements Writable {
    private int type = 0; //数据类型1:User Click history,2:Predict User
    private String imei = "";
    private String aid = ""; //App文章ID
    private int click = 0;

    public UserClick() {
        super();
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public void setClick(int click) {
        this.click = click;
    }

    public UserClick(UserClick uItem) {
        this.type = uItem.type;
        this.imei = uItem.imei;
        this.aid = uItem.aid;
        this.click = uItem.click;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.type);
        out.writeUTF(this.imei);
        out.writeUTF(this.aid);
        out.writeInt(this.click);
    }

    public void readFields(DataInput in) throws IOException {
        this.type = in.readInt();
        this.imei = in.readUTF();
        this.aid = in.readUTF();
        this.click = in.readInt();
    }

    public String toString() {
        return this.type + "\t" + this.imei + "\t" + this.aid + "\t" + this.click;
    }

    public int getType() {
        return type;
    }

    public String getImei() {
        return imei;
    }

    public String getAid() {
        return aid;
    }

    public int getClick() {
        return click;
    }
}
