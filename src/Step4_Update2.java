
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4_Update2 {

	public static class Step4_RecommendMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			Text k = new Text(tokens[0]);
			Text v = new Text(tokens[1] + "," + tokens[2] + "," + tokens[3]);// change
			context.write(k, v);
		}
	}
  //统计出用户对电影的预测评分，并将利用归一化的思想将其评分至0-5，同时避免俩俩电影出现次数过多，但评分不大，却造成结果较大的不合理因素。

	public static class Step4_RecommendReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println(key.toString() + ":");
			Map<String, Double> map = new HashMap<String, Double>();
			Map<String, Double> map2 = new HashMap<String, Double>();// change
			for (Text line : values) {
				System.out.println(line.toString());
				String[] tokens = Recommend.DELIMITER.split(line.toString());
				String itemID = tokens[0];
				Double score = Double.parseDouble(tokens[1]);
				Double num = Double.parseDouble(tokens[2]);// change

				if (map.containsKey(itemID)) {
					if (score > 0) {
						map.put(itemID, map.get(itemID) + score);
						map2.put(itemID, map2.get(itemID) + num);// change
					}
				} else {
					if (score > 0) {
						map.put(itemID, score);
						map2.put(itemID, num);// change
					}

				}
			}
            //进行归一化，求得相应的评分。
			Iterator<String> iter = map.keySet().iterator();
			while (iter.hasNext()) {
				String itemID = iter.next();
				double score = map.get(itemID);
				double num = map2.get(itemID);
				
				double endResult = score / num;
				
				int getInt = (int) endResult;
				endResult = endResult - getInt;
				if (endResult < 0.25) {
					endResult = (double) getInt;
				} else if (endResult < 0.5) {
					endResult = (double) getInt + 0.5;
				} else if (endResult < 0.75) {
					endResult = (double) getInt + 0.5;
				} else {
					endResult = (double) getInt + 1.0;
				}
				Text v = new Text(itemID + ":" + endResult);
				context.write(key, v);
			}
		}
	}

	public static void run(Map<String, String> path) throws IOException,
			InterruptedException, ClassNotFoundException {
		System.out.println("Step4_Update2");

		JobConf conf = Recommend.config();

		String input = path.get("Step4_2Input");
		String output = path.get("Step4_2Output");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		hdfs.rmr(output);

		Job job = new Job(conf);
		job.setJarByClass(Step4_Update2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step4_Update2.Step4_RecommendMapper.class);
		job.setReducerClass(Step4_Update2.Step4_RecommendReducer.class);
		
		job.getConfiguration().set("mapred.textoutputformat.separator", ":");


		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}
