package com.netease.dadaOffline.statistics;

import java.io.IOException;
import java.net.URL;
import java.util.Map;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.dadaOffline.common.DaDaConstants;
import com.netease.dadaOffline.data.DirConstant;

import com.netease.dadaOffline.utils.DadaUtils;
import com.netease.dadaOffline.utils.HadoopUtils;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.DirUtils;

public class DailyArticleShareBackCountMR extends MRJob {

    @Override
    public boolean init(String date) {
        inputList.add(DirUtils.getMobilePartitionPath(DirConstant.MOBILE_HIVELOG, date));
        inputList.add(DirConstant.COMMON_DATA_DIR+"devilfishLogClassification/shareBackLog_sps/"+date);
        outputList.add(DirConstant.STATISTICS_TODC_DIR + "articleShareBackCount/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, MobileHiveMapper.class);
        MultipleInputs.addInputPath(job1, new Path(inputList.get(1)), TextInputFormat.class, ZyLogMapper.class);
        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setCombinerClass(ArticleShareBackCountReducer.class);

        // reducer
        job1.setReducerClass(ArticleShareBackCountReducer.class);
        job1.setNumReduceTasks(2);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if (!job1.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }

    public static class MobileHiveMapper extends Mapper<BytesWritable, Text, Text, IntWritable> {
        private Text  outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        private final static String[] appids = {"2x1kfBk63z", "2S5Wcx"}; // Android,iPhone
        private final static String eventname = "SHARE_NEWS";

        @Override
        public void map(BytesWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            try {
                String infos[] = line.split("\001");
                if (infos.length != 22) {
                    context.getCounter("MobileHiveMapper", "not24").increment(1);
                } else {
                    if (java.util.Arrays.asList(appids).contains(infos[3]) && eventname.equals(infos[4])
                            && DadaUtils.isDada(infos[5])) {
                        int acc = Integer.parseInt(infos[15]);

                        outputKey.set("share_"+DaDaConstants.column_all);
             
                        outputValue.set(acc);
                        context.write(outputKey, outputValue);
                    }
                }
            } catch (Exception e) {
                context.getCounter("MobileHiveMapper", "mapException").increment(1);
            }
        }
    }

    
    public static class ZyLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text  outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);
    	private LogParser parser = new ZylogParser(); 

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try { 
        	String line = value.toString();
        	Map<String, String> map = parser.parse(line);
        	URL url = new URL(map.get(ZyLogParams.url));
        	String query = url.getQuery();
    		String []params = query.split("&");
    		for(String param:params){
    			if(param.split("=").length!=2){
    				continue;
    			}
    			String mapKey = param.split("=")[0];
    			String mapValue = param.split("=")[1];
    			map.put(mapKey,mapValue);
    			
    		}
    		 String docid = map.get("docid");
    		if (docid!=null &&  DadaUtils.isDada(docid)){
    			 outputKey.set("back_"+DaDaConstants.column_all);
                 context.write(outputKey, outputValue);
    		}   
            } catch (Exception e) {
                context.getCounter("ZyLogMapper", "mapException").increment(1);
            }
        }
    }
    public static class ArticleShareBackCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

}
