package com.netease.dadaOffline.common;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;

import com.netease.weblogCommon.utils.HttpClientFactory;

public class DataCubeTool {
    
    public static void sendToDC(String date, String content, String dcHost){
        
        System.out.println("date: " + date);
        System.out.println("content: " + content);
        System.out.println("dcHost=" + dcHost);
        
        if(null == dcHost){
            System.err.println("post status: null dchost");
        }else{
            HttpClient httpClient = HttpClientFactory.getNewInstance();
            
            PostMethod pm = HttpClientFactory.postMethod(dcHost);
            
            pm.addParameter("day", date);
            pm.addParameter("data", content);

            try {
                int status = httpClient.executeMethod(pm);
                System.out.println("post status: " + status);
            } catch (Exception e) {
                System.err.println("post status: error");
            }
        }
    }
}
