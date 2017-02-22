package com.netease.dadaOffline.statistics;

import com.netease.dadaOffline.data.DirConstant;
import com.netease.dadaOffline.utils.DadaUtils;
import com.netease.dadaOffline.utils.HadoopUtils;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by liu_huan on 2016/2/22.
 */
public class DailyArticleIncrCountMR extends MRJob {
    @Override
    public boolean init(String date) {
        inputList.add(DirConstant.CMS_ARTICLE_INCR + date);
        outputList.add(DirConstant.STATISTICS_TODC_DIR + "articleIncrCount/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, ArticleIncrMapper.class);
        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // reducer
        job1.setReducerClass(CountReducer.class);
        job1.setNumReduceTasks(1); //必须为1
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if (!job1.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }
        return jobState;
    }

    public static class ArticleIncrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();

                while (line.contains("\t\t")) {
                    line = line.replace("\t\t", "\t(null)\t");
                }
                if (line.endsWith("\t")) {
                    line = line + "(null)";
                }

                String[] strs = line.split("\t");
                if (strs.length == 15) {
                    String docid = strs[0];
                    if (DadaUtils.isDada(docid)) {
                        outputKey.set(docid);
                        context.write(outputKey, outputValue);
                    }
                } else {
                    context.getCounter("ArticleIncrMapper", "parseLineError").increment(1);
                }
            } catch (Exception e) {
                context.getCounter("ArticleIncrMapper", "mapException").increment(1);
            }
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private int count = 0;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            count++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("count"), new IntWritable(count));
        }
    }

}
