package org.conan.myhadoop.mr.sum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.conan.myhadoop.mr.kpi.KPI;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zhou on 14-5-18.
 */
public class ByteSum {

    public static class ByteSumMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.parser(value.toString());
            if (kpi.isValid()) {
                try{
                    one.set(Integer.parseInt(kpi.getBody_bytes_sent()));
                    word.set(kpi.getRequestURL());
                    output.collect(word, one);
                }catch(Exception exc){

                }

            }
        }
    }

    public static class ByteSumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://192.168.1.40:54310/user/hdfs/log_kpi";
        String output = "hdfs://192.168.1.40:54310/user/hdfs/log_kpi/browser";

        JobConf conf = new JobConf(ByteSum.class);
        conf.setJobName("KPIBrowser");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(ByteSumMapper.class);
        conf.setCombinerClass(ByteSumReducer.class);
        conf.setReducerClass(ByteSumReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        System.exit(0);
    }

}
