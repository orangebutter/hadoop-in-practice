package org.conan.myhadoop.mr.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.conan.myhadoop.mr.ip.IPSeeker;
import org.conan.myhadoop.mr.kpi.KPI;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Scanner;

/**
 * Created by zhou on 14-5-18.
 */
public class KPIRegion {

    public static class KPIRegionMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

        private IntWritable one = new IntWritable(1);
        private Text regions = new Text();
        private String inputFile;

        private IPSeeker ipSeeker;
        @Override
        public void configure(JobConf job) {
            inputFile = job.get("map.input.file");
            Path[] patternsFiles = new Path[0];
            try {
                patternsFiles = DistributedCache.getLocalCacheFiles(job);
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
            }
            ipSeeker = new IPSeeker(patternsFiles[0].toString());
        }
//        @Override
//        protected void setup(Context context)
//                throws IOException, InterruptedException {
//            System.out.println("setup");
////            logger.info("开始启动setup了哈哈哈哈");
//            // System.out.println("运行了.........");
//            Configuration conf=context.getConfiguration();
//            path= DistributedCache.getLocalCacheFiles(conf);
//            System.out.println("获取的路径是：  "+path[0].toString());
//            //  FileSystem fs = FileSystem.get(conf);
//            FileSystem fsopen= FileSystem.getLocal(conf);
//            FSDataInputStream in = fsopen.open(path[0]);
//
//            Scanner scan=new Scanner(in);
//            while(scan.hasNext()){
//                System.out.println(Thread.currentThread().getName()+"扫描的内容:  "+scan.next());
//            }
//            scan.close();
//        }

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.parser(value.toString());
            if (kpi.isValid()) {
                try{
                    regions.set(ipSeeker.getAddress(kpi.getRemote_addr()));
                    output.collect(regions, one);
//                    System.err.println("Success remoteaddress " + kpi.getRemote_addr() + " region " + IPSeeker.getInstance().getAddress(kpi.getRemote_addr()) );
                }catch(Exception exc){
                    System.err.println("Fail remoteaddress " + kpi.getRemote_addr() + " exc " + exc);
                }

            }
        }
    }


    public static class KPIRegionReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
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
//        IPSeeker.qqwryFileURL = args[2];
        String input = "hdfs://192.168.1.40:54310/user/hdfs/log_kpi/";
        String output = "hdfs://192.168.1.40:54310/user/hdfs/log_kpi/ip";

        JobConf conf = new JobConf(KPIRegion.class);
        conf.setJobName("KPIIP");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(KPIRegionMapper.class);
        conf.setCombinerClass(KPIRegionReducer.class);
        conf.setReducerClass(KPIRegionReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        DistributedCache.addCacheFile(new URI("/user/ipseeker/qqwry.dat"), conf);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        System.exit(0);
    }

}
