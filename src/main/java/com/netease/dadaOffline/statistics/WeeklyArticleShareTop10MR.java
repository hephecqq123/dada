package com.netease.dadaOffline.statistics;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
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
import com.netease.dadaOffline.statistics.WeeklyArticleBackTop10MR.ZyLogMapper;
import com.netease.dadaOffline.utils.DadaUtils;
import com.netease.dadaOffline.utils.HadoopUtils;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.DirUtils;
import com.netease.weblogCommon.utils.SimpleTopNTool;
import com.netease.weblogCommon.utils.SimpleTopNTool.SortElement;

public class WeeklyArticleShareTop10MR extends MRJob {

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
        	 inputList.add(DirUtils.getMobilePartitionPath(DirConstant.MOBILE_HIVELOG, d));
        }
       
        outputList.add(DirConstant.STATISTICS_TEMP_DIR + "weeklyArticleShareTop10/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        for (String input : inputList) {
            Path path = new Path(input);
            if (getHDFS().exists(path)) {
                MultipleInputs.addInputPath(job1, path, SequenceFileInputFormat.class, MobileHiveMapper.class);
            }
        }
        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setCombinerClass(ArticleShareCountCombine.class);

        // reducer
        job1.setReducerClass(ArticleShareCountReducer.class);
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
                String device_uuid = infos[6];
                if (infos.length != 22) {
                    context.getCounter("MobileHiveMapper", "not24").increment(1);
                } else {
                    if (java.util.Arrays.asList(appids).contains(infos[3]) && eventname.equals(infos[4])
                            && DadaUtils.isDada(infos[5])) {
                        int acc = Integer.parseInt(infos[15]); 
                        outputKey.set(infos[5]);             
                        outputValue.set(acc);
                        context.write(outputKey, outputValue);
                        outputKey.set(DaDaConstants.column_all);             
                        outputValue.set(acc);
                        context.write(outputKey, outputValue);
                    }
                }
            } catch (Exception e) {
                context.getCounter("MobileHiveMapper", "mapException").increment(1);
            }
        }
    }
    
    public static class ArticleShareCountCombine
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

    public static class ArticleShareCountReducer
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

           // utl.addElement(new SortElement(sum, key.toString()));
        }
//    	@Override
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
