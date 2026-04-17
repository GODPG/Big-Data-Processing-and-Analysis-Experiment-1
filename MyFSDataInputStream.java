import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

public class MyFSDataInputStream extends FSDataInputStream {
    private BufferedReader br;
    public MyFSDataInputStream(InputStream in) 
    {
        super(in);
        this.br = new BufferedReader(new InputStreamReader(in));
    }
    
    public String myReadLine() throws IOException 
    {
        return br.readLine();
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path("/user/hadoop-wei/java_test.txt");
            if (fs.exists(file) == false) 
            {
                System.out.println("找不到文件");
                return;
            }
            InputStream in = fs.open(file);
            MyFSDataInputStream myIn = new MyFSDataInputStream(in);

            System.out.println("调用myReadLine()方法");
            String line = myIn.myReadLine(); 
            
            while (line != null) 
            {
                System.out.println(line); 
                line = myIn.myReadLine(); 
            }
           
            myIn.close();
            fs.close();
            
            System.exit(0); 

        } catch (Exception e) {
            System.out.println("程序运行出错：" + e.getMessage());
        }
    }
}