package com.netease.dadaOffline.data;

public class DirConstant {
    
    public static final String DIR_PREFIX="/ntes_weblog/";
    
    //章鱼日志
    public static final String ZY_LOG = DIR_PREFIX + "163com_zy/";
    //weblog日志
    public static final String WEBLOG_LOG = DIR_PREFIX + "163com/";
    //移动日志
    public static final String MOBILE_LOG = "/user/mobilestat/rawlog/";
    public static final String MOBILE_HIVELOG = "/user/mobilestat/formatlog/event_detail/";
    
    //原始日志层
    public static final String COMMON_DATA_DIR = DIR_PREFIX + "commonData/";
    //跟帖原始数据
    public static final String GEN_TIE_INFO = COMMON_DATA_DIR + "genTieLogNew/";
    //CMS新增文章数据
    public static final String CMS_ARTICLE_INCR = COMMON_DATA_DIR + "articleIncr/";
    public static final String CMS_ARTICLE_ALL = COMMON_DATA_DIR + "articleAll/";

    //统计数据
    public static final String STATISTICS_DIR = DIR_PREFIX + "dada/statistics/";
    
    public static final String STATISTICS_TEMP_DIR = STATISTICS_DIR + "temp/";

    public static final String STATISTICS_TODC_DIR = STATISTICS_DIR + "result_toDC/";

    public static final String STATISTICS_OTHER_DIR = STATISTICS_DIR + "result_other/";


}
