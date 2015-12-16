

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class Recommend {

    public static final String HDFS = "hdfs://master:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) throws Exception {
    	String input = args[0];
    	String output = args[1];
    	
        Map<String, String> path = new HashMap<String, String>();
        path.put("data",input);
        path.put("Step1Input", HDFS + "/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
		//第二步的输入为第一步的输出
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        //第三部的输入为第一步的输出
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
		//第四步的输入为第二步和第三步的输出
        path.put("Step4_1Input1", path.get("Step3Output1"));
		
        path.put("Step4_1Input2", path.get("Step2Output"));
        path.put("Step4_1Output", path.get("Step1Input") + "/step4_1");
        //Step_2的输入是第一步的输出
        path.put("Step4_2Input", path.get("Step4_1Output"));
        path.put("Step4_2Output",output);
        path.put("Step5Input",path.get("Step4_2Output") + "/part-r-00000");
        path.put("Step5Output", output);    
        Step1.run(path);
        Step2.run(path);
        Step3.run(path);
        
        Step4_Update.run(path);
        Step4_Update2.run(path);
        
        Step5.run(path);
        

        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.set("io.sort.factor", "100");
        return conf;
    }

}
