package com.netease.dadaOffline.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import com.netease.weblogCommon.utils.JsonUtils;

public class DataExporter {

    private static final String proPrefix = DaDaConstants.STATISTICS_PREFIX;

    /**
     * args: 结果主存放主目录   日期yyyyMMdd 发送dc地址
     *
     */
	@SuppressWarnings("deprecation")
    public static void main(String[] args) {
        String baseDir = null;
        String date = null;
        String dataCubeHost = null;
        
        if(3 == args.length ){
            baseDir = args[0];
            date = args[1];
            dataCubeHost = args[2];
        } else {
            System.err.println("args error");
            return;
        }
        
        Map<String, String> result = new HashMap<String, String>();
        
        try {
        	Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path baseDirFile = new Path(baseDir);
			
			for(FileStatus fileStatus : fs.listStatus(baseDirFile)){
				try {
					String keyPrefix = proPrefix + fileStatus.getPath().getName();
					
					Text readKey = new Text();
					IntWritable readVal = new IntWritable();
					Path resDirPath = new Path(baseDir + "/" + fileStatus.getPath().getName() + "/" + date + "/");
					if(!fs.exists(resDirPath)){
						continue;
					}
					for(FileStatus resFileStatus : fs.listStatus(resDirPath)){
						Path resFilePath = resFileStatus.getPath();
						if(resFilePath.getName().startsWith("p")){
							SequenceFile.Reader reader = null;
							try {
								reader = new Reader(fs, resFilePath, conf);
								while(reader.next(readKey, readVal)){
									String dbKey = keyPrefix + "_" + readKey.toString(); 
									result.put(dbKey, readVal.toString());
								}
							} catch (Exception e) {
								e.printStackTrace();
							} finally{
								try {
									if(null != reader){
										reader.close();
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        String resultJson = JsonUtils.toJson(result);
        String dayStr = date.substring(0,4) + "-" + date.substring(4,6) + "-" + date.substring(6,8);
        
        DataCubeTool.sendToDC(dayStr, resultJson, dataCubeHost);
 
        System.exit(0);

    }

}

