package com.netease.dadaOffline.utils;

import org.apache.commons.lang3.StringUtils;

import com.netease.weblogCommon.utils.DocidUtils;

public class DadaUtils {
	//独家抓取栏目：00014U9R；哒哒良品 000155IU；第三方 000155K8；哒哒进口 000155IV；转载与精编稿库 00014TUH；酷酷哒 000158CR；易奇闻栏目 00014Q2A；人家 00015688
	private static final String[] dadaTopics = {"00014U9R","000155IU","000155K8","000155IV","00014TUH","000158CR","00014Q2A","00015688"};
	
	public static boolean isDada(String docid){
		String topic = DocidUtils.getTopicId(docid);
		if(StringUtils.isNotBlank(topic)){
			for(String t : dadaTopics){
				if(topic.equals(t)){
					return true;
				}
			}
		}
		return false;
	}
}
