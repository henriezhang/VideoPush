package com.webdev.test;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by henriezhang on 2014/8/6.
 */
public class KeywordTest {
    public static void main(String[] args) throws Exception {
        System.out.println("Begin test");
        String[] res = new String[20];
        try {
            // POST的URL
            HttpPost httppost = new HttpPost("http://10.129.138.54:8081");
            // 建立HttpPost对象
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            // 建立一个NameValuePair数组，用于存储欲传送的参数
            params.add(new BasicNameValuePair("op", "keyword"));
            params.add(new BasicNameValuePair("weight", "false"));
            params.add(new BasicNameValuePair("count", "20"));
            params.add(new BasicNameValuePair("title", ""));
            params.add(new BasicNameValuePair("content", "霍华德的16篮板，直接带动火箭于篮板球数据上51-41压制了马刺。" +
                    "火箭就此胆气喷薄——三分球38投13中，效率其实并不算出色，但敢于远投出手38次，球队特色绝对尽显。" +
                    "马刺远投则是18投7中。火箭的防守效果也很直观——邓肯此战12投仅2中，丹尼-格林13投5中也不理想，" +
                    "马刺88投仅35中（命中率不及40%）；火箭89投40中，命中率接近45%。"));
            // 设置编码
            httppost.setEntity(new UrlEncodedFormEntity(params, HTTP.UTF_8));
            // 发送Post,并返回一个HttpResponse对象
            HttpResponse response = new DefaultHttpClient().execute(httppost);
            // 如果状态码为200,就是正常返回
            if(response.getStatusLine().getStatusCode()==200) {
                String result = EntityUtils.toString(response.getEntity());
                // 得到返回的字符串
                System.out.println(result);

                // 解析json数据
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(result);
                JsonNode keywords = rootNode.path("keyword");
                System.out.println("AAA");
                for(int i=0; i<keywords.size(); i++) {
                    System.out.println(keywords.get(i).toString());
                }
                System.out.println("BBB");
            }
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
        System.out.println("End test");
    }
}
