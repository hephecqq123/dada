package com.netease.dadaOffline.statistics;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.SimpleTopNTool;
import com.netease.weblogCommon.utils.SimpleTopNTool.SortElement;


public class WeeklyArticleBackTop10MR extends MRJob {

    @Override
    public boolean init(String date) {
    	List<String> dateList;
        try {
            String firstDay = DateUtils.getTheDayBefore(date, 6);
            dateList = DateUtils.getDateList(firstDay, date);
        } catch (ParseException e) {
            return false;
        }
        for (String d : dateList) {
        	 inputList.add(DirConstant.COMMON_DATA_DIR+"devilfishLogClassification/shareBackLog_sps/"+d);
        }
       
        outputList.add(DirConstant.STATISTICS_TEMP_DIR + "weeklyArticleBackTop10/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        for (String input : inputList) {
            Path path = new Path(input);
            if (getHDFS().exists(path)) {
                MultipleInputs.addInputPath(job1, path, TextInputFormat.class, ZyLogMapper.class);
            }
        }
        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setCombinerClass(ArticleBackCountCombine.class);

        // reducer
        job1.setReducerClass(ArticleBackCountReducer.class);
        job1.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if (!job1.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        return jobState;
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
        	String surl = map.get(ZyLogParams.url);
        	URL url = new URL(surl);
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
    		if (docid!=null &&  DadaUtils.isDada(docid)&&!map.containsKey("func")){
    			 outputKey.set(docid);
                 context.write(outputKey, outputValue);
            	 outputKey.set(DaDaConstants.column_all);
                 context.write(outputKey, outputValue);
    		}   
            } catch (Exception e) {
                context.getCounter("ZyLogMapper", "mapException").increment(1);
            }
        }
    }
    
    public static class ArticleBackCountCombine
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

    public static class ArticleBackCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
    	 private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();
        SimpleTopNTool utl = new SimpleTopNTool(11);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            
	    	  outputValue.set(sum);
	    	  outputKey.set(key.toString());
	            context.write(outputKey, outputValue);
	     //   utl.addElement(new SortElement(sum, key.toString()));
        }
//		@Override
//		protected void cleanup(
//				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			
//			super.cleanup(context);
//		      for(SortElement ele : utl.getTopN()){
//		        	
//		    	  outputValue.set((int)ele.getCount());
//		    	  outputKey.set(ele.getVal().toString());
//		            context.write(outputKey, outputValue);
//				}
//		}
        
        
    }

}
