package com.netease.dadaOffline.statistics;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
import com.netease.dadaOffline.utils.DadaUtils;
import com.netease.dadaOffline.utils.HadoopUtils;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DirUtils;

public class DailyColumnPvUvMR extends MRJob {

    @Override
    public boolean init(String date) {
        inputList.add(DirUtils.getMobilePartitionPath(DirConstant.MOBILE_HIVELOG, date));
        outputList.add(DirConstant.STATISTICS_TEMP_DIR + "columnPvUvTemp/" + date);
        outputList.add(DirConstant.STATISTICS_TODC_DIR + "columnPvUv/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, MobileHiveMapper.class);
        // mapper
        job1.setMapOutputKeyClass(StringStringWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setCombinerClass(PvUvTempReducer.class);

        // reducer
        job1.setReducerClass(PvUvTempReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(StringStringWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        if (!job1.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, PvUvMapper.class);

        // mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        // reducer
        job2.setReducerClass(PvUvReducer.class);
        job2.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        if (!job2.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }

    public static class MobileHiveMapper extends Mapper<BytesWritable, Text, StringStringWritable, IntWritable> {
        private StringStringWritable outputKey = new StringStringWritable();
        private IntWritable outputValue = new IntWritable();

        private final static String[] appids = {"2x1kfBk63z", "2S5Wcx"}; // Android,iPhone
        private final static String eventname = "_pvX";

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
                        String column = infos[16];
                        String device_uuid = infos[6];
                        int acc = Integer.parseInt(infos[15]);

                        if (StringUtils.isNotBlank(column) && column.contains("哒哒")) {
                            column = "哒哒";
                        }
                        String columnKey = DaDaConstants.columnsMap.get(column);
                        if (columnKey != null) {
                            outputKey.setFirst(columnKey);
                            outputKey.setSecond(device_uuid);
                            outputValue.set(acc);
                            context.write(outputKey, outputValue);
                        } else {
                            outputKey.setFirst(DaDaConstants.column_other);
                            outputKey.setSecond(device_uuid);
                            outputValue.set(acc);
                            context.write(outputKey, outputValue);
                        }

                        outputKey.setFirst(DaDaConstants.column_all);
                        outputKey.setSecond(device_uuid);
                        outputValue.set(acc);
                        context.write(outputKey, outputValue);
                    }
                }
            } catch (Exception e) {
                context.getCounter("MobileHiveMapper", "mapException").increment(1);
            }
        }
    }

    public static class PvUvTempReducer
            extends Reducer<StringStringWritable, IntWritable, StringStringWritable, IntWritable> {
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(StringStringWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    public static class PvUvMapper extends Mapper<StringStringWritable, IntWritable, Text, IntWritable> {
        private Text outputKey = new Text();

        @Override
        protected void map(StringStringWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            outputKey.set(key.getFirst());
            context.write(outputKey, value);
        }
    }

    public static class PvUvReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int pv = 0;
            int uv = 0;
            for (IntWritable val : values) {
                pv += val.get();
                uv++;
            }
            outputKey.set(key.toString() + "_pv");
            outputValue.set(pv);
            context.write(outputKey, outputValue);
            outputKey.set(key.toString() + "_uv");
            outputValue.set(uv);
            context.write(outputKey, outputValue);
        }

    }

}
