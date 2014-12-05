package com.webdev.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by henriezhang on 2014/4/5.
 */

public class VideoClick implements Writable {
    public final static int CLICK = 1; // 表示有点击
    public final static int NOCLICK = 0; // 表示没有点击

    private String uin = "";
    private String id = ""; // 视频ID或者专辑ID
    private int click = 0;

    public VideoClick() {
        super();
    }

    public VideoClick(VideoClick v) {
        this.uin = v.uin;
        this.id = v.id;
        this.click = v.click;
    }

    public VideoClick(String uin, String id, String click) {
        this.uin = uin;
        this.id = id;
        try{
            this.click = Integer.parseInt(click);
        } catch (Exception e){
            this.click = 0;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uin);
        out.writeUTF(this.id);
        out.writeInt(this.click);
    }

    public void readFields(DataInput in) throws IOException {
        this.uin = in.readUTF();
        this.id = in.readUTF();
        this.click = in.readInt();
    }

    public String getId() {
        return id;
    }

    public String getUin() {
        return uin;
    }

    public int getClick() {
        return click;
    }

    public void setUin(String uin) {
        this.uin = uin;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setClick(int click) {
        this.click = click;
    }

    public void setClick(String click) {
        try{
            this.click = Integer.parseInt(click);
        } catch (Exception e){
            this.click = 0;
        }
    }

    public String toString() {
        return this.uin + "\t" + this.id + "\t" + this.click;
    }
}
