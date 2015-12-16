

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

//旧版API
public class Step1 {
	public static final Pattern DELIMITER = Pattern.compile("::");
    public static class Step1_ToItemPreMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
		
        /**
         * map¹¹½¨Ã¿¸öuser¶ÔµçÓ°µÄÆÀ¼Û¡£
         * @param key
         * @param value
         * @param output
         * @param reporter
         * @throws IOException
         */
        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Step1.DELIMITER.split(value.toString());
            int userID = Integer.parseInt(tokens[0]);
            String itemID = tokens[1];
            String pref = tokens[2];
            k.set(userID);
            v.set(itemID + ":" + pref);
            output.collect(k, v);
        }
    }
	/**
	 * ½øÐÐreduce£¬½«map³öÀ´µÄÍ¬Ò»¸öÓÃ»§µÄµçÓ°ÆÀ¼Û½øÐÐºÏ²¢£¬´Ó¶ø¹¹½¨³öÍêÕûµÄÃ¿¸öÓÃ»§¶ÔµçÓ°µÄÆÀ¼Û¡£
	 * @author wei
	 *
	 */
    public static class Step1_ToUserVectorReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                sb.append("," + values.next());
            }
            v.set(sb.toString().replaceFirst(",", ""));
            output.collect(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException {
    	System.out.println("Step1");
    	
        JobConf conf = Recommend.config();

        String input = path.get("Step1Input");
        String output = path.get("Step1Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("data"), input);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step1_ToItemPreMapper.class);
        conf.setCombinerClass(Step1_ToUserVectorReducer.class);
        conf.setReducerClass(Step1_ToUserVectorReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

}
