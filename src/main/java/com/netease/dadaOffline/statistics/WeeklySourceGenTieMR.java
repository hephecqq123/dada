package com.netease.dadaOffline.statistics;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

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

import com.netease.dadaOffline.common.DaDaConstants;
import com.netease.dadaOffline.data.DirConstant;
import com.netease.dadaOffline.utils.DadaUtils;
import com.netease.dadaOffline.utils.HadoopUtils;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DateUtils;

public class WeeklySourceGenTieMR extends MRJob {

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
            inputList.add(DirConstant.GEN_TIE_INFO + d);
        }
        outputList.add(DirConstant.STATISTICS_TEMP_DIR + "genTieCount/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

        for (String input : inputList) {
            Path path = new Path(input);
            if (getHDFS().exists(path)) {
                MultipleInputs.addInputPath(job1, path, TextInputFormat.class, GenTieInfoMapper.class);
            }
        }

        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // reducer
        job1.setReducerClass(GenTieCountReducer.class);
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

    public static class GenTieInfoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text outputKey = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                // url,pdocid,docid,发帖用户id,跟帖时间，跟帖id,ip,source
                String[] strs = value.toString().split(",");
                String url = strs[0];
                String docid = strs[2];
                String source = strs[7];
                if (DadaUtils.isDada(docid)) {
                    String sourceKey = DaDaConstants.sourcesMap.get(source);
                    if (sourceKey != null) {
                        outputKey.set(sourceKey + "\t" + url);
                        context.write(outputKey, one);
                    }
                    outputKey.set(DaDaConstants.source_all + "\t" + url);
                    context.write(outputKey, one);
                }
            } catch (Exception e) {
                context.getCounter("GenTieInfoMapper", "mapException").increment(1);
            }
        }
    }

    public static class GenTieCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

}
