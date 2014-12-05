
package com.webdev.test.news;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by henriezhang on 2014/8/12.
 */

public class NewsUserClick implements Writable {
    private String uin = "";
    private String aid = ""; //App文章ID
    private int click = 0;

    public NewsUserClick() {
        super();
    }

    public NewsUserClick(NewsUserClick item)
    {
        this.uin = item.getUin();
        this.aid = item.getAid();
        this.click = item.getClick();
    }

    public void setUin(String uin) {
        this.uin = uin;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public void setClick(int click) {
        this.click = click;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uin);
        out.writeUTF(this.aid);
        out.writeInt(this.click);
    }

    public void readFields(DataInput in) throws IOException {
        this.uin = in.readUTF();
        this.aid = in.readUTF();
        this.click = in.readInt();
    }

    public String toString() {
        return this.uin + "\t" + this.aid + "\t" + this.click;
    }

    public String getUin() {
        return uin;
    }

    public String getAid() {
        return aid;
    }

    public int getClick() {
        return click;
    }
}