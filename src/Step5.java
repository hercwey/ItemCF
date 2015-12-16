

import java.io.FileOutputStream;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
*
* Description: �鿴Hadoop�ļ�ϵͳ�е��ļ�������hadoop FileSystem�ӿ��е�FSDataInputStream
* FSDataInputStream����������λ�����������Դ��ļ�������λ�ÿ�ʼ��ȡ
*
*/
public class Step5 {
    /**
     * @param args
     */
    public static void run(Map<String, String> path) throws Exception{
         	
    	System.out.println("Step5");
        //��һ���������ݽ�������hadoop�ļ�ϵͳ�е�ĳ���ļ���URI,��hdfs://ip ��theme��ͷ
        String hdfsurl = path.get("Step5Input");
        String outputPath = path.get("Step5Output");
        System.out.println(hdfsurl);
        System.out.println(outputPath);
        //��ȡhadoop�ļ�ϵͳ������
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");
         
        //FileSystem���û�����HDFS�ĺ����࣬�����URI��Ӧ��HDFS�ļ�ϵͳ
        FileSystem fs = FileSystem.get(URI.create(hdfsurl),conf);
        FSDataInputStream in = null;
        FileOutputStream file=new FileOutputStream(outputPath);
        
        try{
           
            //��FileSystem��һ��uri��Ӧ��FSDataInputStream�ļ�����������ȡ����ļ�
            in = fs.open( new Path(hdfsurl) );
            //��Hadoop��IOUtils���߷�����������ļ���ָ���ֽڸ��Ƶ���׼�������
            IOUtils.copyBytes(in, file,1024,false);   //1024Ϊ��������С
            System.out.println();
            
        }finally{
            IOUtils.closeStream(in);
        }
    }
}    

