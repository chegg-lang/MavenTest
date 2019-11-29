import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClient {

        @Test
        public void put() throws IOException, Exception {
          //获取文件系统
            Configuration entries = new Configuration();
            entries.set("dfs.replication","2");
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "atguigu");
            //2.上传文件
            fs.copyFromLocalFile(new Path("d://demo/source/考试文件.txt"),new Path("/source/kaoshi.txt"));
            //3.关闭资源
            fs.close();
            System.out.println("数据上传完成");
        }

        @Test
    public void delete() throws Exception {
            //1.获取文件系统

            Boolean flag=true;
            //1.1获取配置文件的对象
            Configuration cfg = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), cfg, "atguigu");
            //2.执行删除
            fs.delete(new Path("/tangseng"),true);
            //3.关闭资源
            fs.close();
            }

            @Test
    public void rename() throws Exception {
            //获取配置文件的对象
                Configuration cfg = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), cfg, "atguigu");

                //2.重命名操作
                boolean rename = fs.rename(new Path("/meinv.jpg"), new Path("/reba.jpg"));
                if (rename) {
                    System.out.println("更名成功");
                }else{
                    System.out.println("更名不成功");
                }

                //3.关闭资源
                fs.close();
        }
        @Test
    public void pull() throws IOException, Exception {
            Configuration cfg = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), cfg, "atguigu");

            //下载文件
            fs.copyToLocalFile(new Path("/README.txt"),new Path("d://demo/"));
            //关闭资源
            fs.close();
        }

        @Test
    public void checkfile() throws Exception {
            Configuration cfg = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), cfg, "atguigu");

            //获取文件的详情
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
            int count=1;
            while (listFiles.hasNext()) {
                //获取文件对象
                LocatedFileStatus next = listFiles.next();
                if (next.isFile()) {
                    System.out.println("###########################第" + count + "个文件##################################");
                    String name = next.getPath().getName();
                    System.out.println(name);
                    System.out.println(next.getPermission());
                    System.out.println("###############################################################");
                } else if(next.isDirectory()){
                    System.out.println("******************************第" + count + "个文件夹******************************");

                    //获取文件名称
                    System.out.println(next.getPath().getName());
                    //获取文件的长度
                    System.out.println(next.getLen());
                    //获取文件的权限
                    System.out.println(next.getPermission());
                    //获取文件的所有者
                    System.out.println(next.getOwner());

                    //获取文件储存块信息
                    BlockLocation[] locations = next.getBlockLocations();
                    for (BlockLocation location : locations) {
                        String[] hosts = location.getHosts();
                        for (String host : hosts) {
                            System.out.println(host);
                        }
                    }
                    System.out.println("*******************************************************************************");
                }
                count++;
            }
        }

@Test
    public void listStatus() throws Exception {
    Configuration cfg = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),cfg, "atguigugu");
    FileStatus[] status = fs.listStatus(new Path("/"));
        for (FileStatus fst : status) {
            if (fst.isFile()) {
                System.out.println("这是文件，文件名为："+fst.getPath().getName());
            }else{
                System.out.println("这是文件夹，文件夹名为："+fst.getPath().getName());
            }
        }

    }
}


