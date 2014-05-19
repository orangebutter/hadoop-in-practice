package org.conan.myhadoop.mr.sum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.conan.myhadoop.mr.kpi.KPI;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by zhou on 14-5-18.
 */
public class DomainIP {

    public static class DomainIPMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text ips = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.parser(value.toString());
            if (kpi.isValid()) {
                if(!"-".equals(kpi.getHttp_referer_domain())){
                    word.set(kpi.getHttp_referer_domain());
                    ips.set(kpi.getRemote_addr());
                    output.collect(word, ips);
                }
            }
        }
    }


    public static class DomainIPReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Set<String> count = new HashSet<String>();

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                count.add(values.next().toString());
            }
            result.set(String.valueOf(count.size()));
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://192.168.1.40:54310/user/hdfs/log_kpi/";
        String output = "hdfs://192.168.1.40:54310/user/hdfs/log_kpi/ip";

        JobConf conf = new JobConf(DomainIP.class);
        conf.setJobName("KPIIP");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(DomainIPMapper.class);
//        conf.setCombinerClass(KPIIPReducer.class);
        conf.setReducerClass(DomainIPReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        System.exit(0);
    }

}
