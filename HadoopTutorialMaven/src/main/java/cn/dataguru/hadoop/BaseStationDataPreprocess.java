package cn.dataguru.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

//λ������
//IMSI|IMEI|UPDATETYPE|CGI|TIME
//��������
//IMSI|IMEI|CGI|TIME|CGI|URL

/**
 * ���ܻ�վ���ݱ�
 * ����ÿ���û��ڲ�ͬ��ʱ��β�ͬ�Ļ�վͣ����ʱ��
 * ������� < input path > < output path > < date > < timepoint >
 * ����ʾ���� ��/base /output 2012-09-12 09-17-24"
 * ��ζ���ԡ�/base��Ϊ���룬"/output"Ϊ�����ָ������2012��09��12�յ����ݣ�����Ϊ00-07��07-17��17-24����ʱ��
 * �����ʽ ��IMSI|CGI|TIMFLAG|STAY_TIME��
 */
public class BaseStationDataPreprocess extends Configured implements Tool {
    /**
     * ������
     * ���ڼ��������쳣����
     */
    enum Counter {
        TIMESKIP,        //ʱ���ʽ����
        OUTOFTIMESKIP,    //ʱ�䲻�ڲ���ָ����ʱ�����
        LINESKIP,        //Դ�ļ�������
        USERSKIP        //ĳ���û�ĳ��ʱ��α���������
    }

    /**
     * ��ȡһ������
     * �ԡ�IMSI+ʱ��Ρ���Ϊ KEY �����ȥ
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private String date;
        private String[] timepoint;
        private boolean dataSource;

        /**
         * ��ʼ��
         */
        @Override
        public void configure(org.apache.hadoop.mapred.JobConf job) {
            this.date = job.get("date");                            //��ȡ����
            this.timepoint = job.get("timepoint").split("-");    //��ȡʱ��ָ��

            //��ȡ�ļ���
//			FileSplit fs = (FileSplit)job.getInputSplit();
            String fileName = job.get("map.input.file");
            if (fileName.startsWith("POS"))
                dataSource = true;
            else if (fileName.startsWith("NET"))
                dataSource = false;

        }


        /**
         * MAP����
         * ��ȡ��վ����
         * �ҳ���������Ӧʱ���
         * ��IMSI��ʱ�����Ϊ KEY
         * CGI��ʱ����Ϊ VALUE
         */
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String line = value.toString();
            TableLine tableLine = new TableLine();

            //��ȡ��
            try {
                tableLine.set(line, this.dataSource, this.date, this.timepoint);
            } catch (LineException e) {
                if (e.getFlag() == -1)
                    reporter.getCounter(Counter.OUTOFTIMESKIP).increment(1);
                else
                    reporter.getCounter(Counter.TIMESKIP).increment(1);
                return;
            } catch (Exception e) {
                reporter.getCounter(Counter.LINESKIP).increment(1);
                return;
            }

            outputCollector.collect(tableLine.outKey(), tableLine.outValue());
        }
    }


    /**
     * ͳ��ͬһ��IMSI��ͬһʱ���
     * �ڲ�ͬCGIͣ����ʱ��
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {
        private String date;
        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * ��ʼ��
         */
        @Override
        public void configure(org.apache.hadoop.mapred.JobConf job) {
            this.date = job.get("date");
        }

        public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            String imsi = key.toString().split("\\|")[0];
            String timeFlag = key.toString().split("\\|")[1];

            //��һ��TreeMap��¼ʱ��
            TreeMap<Long, String> uploads = new TreeMap<Long, String>();
            String valueString;

            while (values.hasNext()) {
                valueString = values.next().toString();
                try {
                    uploads.put(Long.valueOf(valueString.split("\\|")[1]), valueString.split("\\|")[0]);
                } catch (NumberFormatException e) {
                    reporter.getCounter(Counter.TIMESKIP).increment(1);
                    continue;
                }
            }

            try {
                //�������ӡ�OFF��λ��
                Date tmp = this.formatter.parse(this.date + " " + timeFlag.split("-")[1] + ":00:00");
                uploads.put((tmp.getTime() / 1000L), "OFF");

                //��������
                HashMap<String, Float> locs = getStayTime3(uploads);

                //���
                for (Entry<String, Float> entry : locs.entrySet()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(imsi).append("|");
                    builder.append(entry.getKey()).append("|");
                    builder.append(timeFlag).append("|");
                    builder.append(entry.getValue());

                    output.collect(NullWritable.get(), new Text(builder.toString()));
                }
            } catch (Exception e) {
                reporter.getCounter(Counter.USERSKIP).increment(1);
                return;
            }
        }
        /**
         * ���λ��ǰ����ͣ����Ϣ
         */
        private HashMap<String, Float> getStayTime3(TreeMap<Long, String> uploads) {
            HashMap<String,Float> locs = getStayTime(uploads);
            HashMap<String,Float> result = new HashMap();
            List<String> posList = new ArrayList<String>();
            for(Iterator<String> keyIt = locs.keySet().iterator();keyIt.hasNext();){
                String key = keyIt.next();
                Float time = locs.get(key);
                int i=0;
                boolean findFlag = false;
                for(;i<posList.size();i++){
                   if( locs.get(posList.get(i)) < time){
                       findFlag = true;
                       break;
                   }
                }
                if(findFlag){
                    posList.add(i,key);
                }else{
                    posList.add(key);
                }

            }
            for(int i=0;i<3;i++){
                result.put(posList.get(i),locs.get(posList.get(i)));
            }
            return result;
        }


        /**
         * ���λ��ͣ����Ϣ
         */
        private HashMap<String, Float> getStayTime(TreeMap<Long, String> uploads) {
            Entry<Long, String> upload, nextUpload;
            HashMap<String, Float> locs = new HashMap<String, Float>();
            //��ʼ��
            Iterator<Entry<Long, String>> it = uploads.entrySet().iterator();
            upload = it.next();
            //����
            while (it.hasNext()) {
                nextUpload = it.next();
                float diff = (float) (nextUpload.getKey() - upload.getKey()) / 60.0f;
                if (diff <= 60.0)                                    //ʱ�������������ػ�
                {
                    if (locs.containsKey(upload.getValue()))
                        locs.put(upload.getValue(), locs.get(upload.getValue()) + diff);
                    else
                        locs.put(upload.getValue(), diff);
                }
                upload = nextUpload;
            }
            return locs;
        }

    }

    @Override
    public int run(String[] args) throws Exception {
//        Configuration conf = getConf();
//
//        conf.set("date", args[2]);
//        conf.set("timepoint", args[3]);

        JobConf job = new JobConf(BaseStationDataPreprocess.class);
        job.set("timepoint", args[3]);
        job.set("date", args[2]);
        job.setJobName("BaseStationDataPreprocess");
        job.setJarByClass(BaseStationDataPreprocess.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));            //����·��
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        //���·��

        job.setMapperClass(Map.class);                                //��������Map����ΪMap�������
        job.setReducerClass(Reduce.class);                            //��������Reduce����ΪReduce�������
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(job);
        return 0;
        //   return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.err.println(" args size " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.err.println(" args  " + i + " value " + args[i]);
        }
        if (args.length != 4) {
            System.err.println("");
            System.err.println("Usage: BaseStationDataPreprocess < input path > < output path > < date > < timepoint >");
            System.err.println("Example: BaseStationDataPreprocess /user/james/Base /user/james/Output 2012-09-12 07-09-17-24");
            System.err.println("Warning: Timepoints should be begined with a 0+ two digit number and the last timepoint should be 24");
            System.err.println("Counter:");
            System.err.println("\t" + "TIMESKIP" + "\t" + "Lines which contain wrong date format");
            System.err.println("\t" + "OUTOFTIMESKIP" + "\t" + "Lines which contain times that out of range");
            System.err.println("\t" + "LINESKIP" + "\t" + "Lines which are invalid");
            System.err.println("\t" + "USERSKIP" + "\t" + "Users in some time are invalid");
            System.exit(-1);
        }

        //��������
        int res = ToolRunner.run(new Configuration(), new BaseStationDataPreprocess(), args);

        System.exit(res);
    }
}