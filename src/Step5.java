

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
* Description: 查看Hadoop文件系统中的文件，利用hadoop FileSystem接口中的FSDataInputStream
* FSDataInputStream还具有流定位的能力，可以从文件的任意位置开始读取
*
*/
public class Step5 {
    /**
     * @param args
     */
    public static void run(Map<String, String> path) throws Exception{
         	
    	System.out.println("Step5");
        //第一个参数传递进来的是hadoop文件系统中的某个文件的URI,以hdfs://ip 的theme开头
        String hdfsurl = path.get("Step5Input");
        String outputPath = path.get("Step5Output");
        System.out.println(hdfsurl);
        System.out.println(outputPath);
        //读取hadoop文件系统的配置
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");
         
        //FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
        FileSystem fs = FileSystem.get(URI.create(hdfsurl),conf);
        FSDataInputStream in = null;
        FileOutputStream file=new FileOutputStream(outputPath);
        
        try{
           
            //让FileSystem打开一个uri对应的FSDataInputStream文件输入流，读取这个文件
            in = fs.open( new Path(hdfsurl) );
            //用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
            IOUtils.copyBytes(in, file,1024,false);   //1024为缓冲区大小
            System.out.println();
            
        }finally{
            IOUtils.closeStream(in);
        }
    }
}    

