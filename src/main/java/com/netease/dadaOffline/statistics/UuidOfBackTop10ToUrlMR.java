package com.netease.dadaOffline.statistics;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.dadaOffline.common.DaDaConstants;
import com.netease.dadaOffline.data.DirConstant;
import com.netease.dadaOffline.data.StringStringWritable;
import com.netease.dadaOffline.utils.HadoopUtils;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;



public class UuidOfBackTop10ToUrlMR extends MRJob {

    @Override
    public boolean init(String date) {

        inputList.add(DirConstant.COMMON_DATA_DIR +"docidAndUrlOfRarticle/"+ date);
        inputList.add(DirConstant.STATISTICS_TEMP_DIR + "weeklyArticleBackTop10/" + date);
        outputList.add(DirConstant.STATISTICS_TEMP_DIR + "weeklyArticleBackTop10Url/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, ArticleMapper.class);
        MultipleInputs.addInputPath(job1, new Path(inputList.get(1)), SequenceFileInputFormat.class, BackTop10Mapper.class);
        
        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(StringStringWritable.class);


        // reducer
        job1.setReducerClass(ArticleBackCountReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if (!job1.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }
    
    
    public static class ArticleMapper extends Mapper<Text, Text, Text, StringStringWritable> {
		private StringStringWritable  outValue = new StringStringWritable();
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	        outValue.setFirst("url");
			outValue.setSecond(value.toString());
			context.write(key, outValue);//<docid,url>
		}
	}

    public static class BackTop10Mapper extends Mapper<Text, IntWritable, Text, StringStringWritable> {
        private StringStringWritable outputValue = new StringStringWritable();

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        	
        	
        	outputValue.setFirst("int");
        	outputValue.setSecond(value.toString());
        	context.write(key, outputValue);//<docid,count>
        }
    }

    public static class ArticleBackCountReducer
            extends Reducer<Text, StringStringWritable, Text, IntWritable> {
    	private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<StringStringWritable> values, Context context)
                throws IOException, InterruptedException {
            String url=null;
            int count = 0;
            boolean burl = false;
            boolean bint = false;
            for (StringStringWritable val : values) {
            	
            	String value = val.getSecond();
            	if (key.toString().equals(DaDaConstants.column_all)){
            		count = Integer.parseInt(value);
            		outputValue.set(count);
            		context.write(key, outputValue);
            	}
            	
                if (val.getFirst().equals("url")){
                	url =value;
                	burl = true;
                }else if (val.getFirst().equals("int")){
                	count = Integer.parseInt(value);
                	bint = true;
                }
            }
            if ((burl && bint)){
            	 outputKey.set(url);
            	 outputValue.set(count);
            	 context.write(outputKey, outputValue);
            }
          
        }
    }

}
