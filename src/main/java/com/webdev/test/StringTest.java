package com.webdev.test;

/**
 * Created by henriezhang on 2014/8/13.
 */
public class StringTest {
    public static void main(String[] args) throws Exception {
        /*System.out.println("Begin test");
        String id = "XAC2014061905036300";
        System.out.println(id.substring(0,17));*/

        String str = "asdf:1234.123,qwer:45.46";
        String tmp = str.replaceAll(":[\\.0-9]*", "");
        System.out.println(tmp);
    }
}
