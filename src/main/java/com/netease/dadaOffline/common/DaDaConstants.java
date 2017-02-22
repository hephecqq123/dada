package com.netease.dadaOffline.common;

import java.util.HashMap;

public class DaDaConstants {
    
    public static final String STATISTICS_PREFIX = "dada_";
    
    /**
     * 哒哒统计的栏目
     */
    public static final HashMap<String, String> columnsMap = new HashMap<String, String>();
    static{
        columnsMap.put("头条", "toutiao");
        columnsMap.put("热点", "redian");
        columnsMap.put("哒哒", "dada");
    }
    
    public static final String column_other = "other";
    
    public static final String column_all = "all";
    
    /**
     * 哒哒统计的来源
     */
    public static final HashMap<String, String> sourcesMap = new HashMap<String, String>();
    static{
        sourcesMap.put("ph", "khd");
        sourcesMap.put("wb", "pc");
        sourcesMap.put("3g", "wap");
    }
    
    public static final String source_all = "all";
}
