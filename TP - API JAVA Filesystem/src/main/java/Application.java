import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class Application {
    public static void main(String[] args) {
        Configuration cf = new Configuration();
        cf.set("fs.defaultFS","hdfs://localhost:9000");
        cf.set("dfs.replication","1");
        try {
            FileSystem fs = FileSystem.get(cf);
            String dirsPaths [] = {"/BDCC1/CPP/Cours","/BDCC1/CPP/TPs","/BDCC1/JAVA/Cours","/BDCC1/JAVA/TPs"};
            /*for(String s : dirsPaths) {
                Path path = new Path(s);
                fs.mkdirs(path);
            }*/

            String filesPaths [] = {"/BDCC1/CPP/Cours/CoursCPP1.txt","/BDCC1/CPP/Cours/CoursCPP2.txt","/BDCC1/CPP/Cours/CoursCPP3.txt"};
            /*for(String s : filesPaths) {
                Path path = new Path(s);
                FSDataOutputStream fin = fs.create(path);
                fin.writeUTF("hello from : "+s+"\n");
            }*/

            /*for(String s : filesPaths) {
                FSDataInputStream is = fs.open(new Path(s));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String line = null;
                while ((line = bufferedReader.readLine())!=null){
                    System.out.println(line);
                }
                is.close();
            }*/

            /*String targetPath = "/BDCC1/JAVA/Cours";
            Path p2 = new Path(targetPath);
            for(String s : filesPaths) {
                Path p1 = new Path(s);
                fs = p1.getFileSystem(cf);
                FileUtil.copy(fs, p1, fs, p2, false, cf);
            }*/

            /*fs.delete(new Path("/BDCC1/JAVA/Cours/CoursCPP3.txt"),false);
            fs.rename(new Path("/BDCC1/JAVA/Cours/CoursCPP1.txt"),new Path("/BDCC1/JAVA/Cours/CoursJAVA1.txt"));
            fs.rename(new Path("/BDCC1/JAVA/Cours/CoursCPP2.txt"),new Path("/BDCC1/JAVA/Cours/CoursJAVA2.txt"));*/

            /*new File("elyousfi").mkdir();
            String paths [] = {"TP1CPP.txt","TP2CPP.txt","TP1JAVA.txt","TP2JAVA.txt","TP3JAVA.txt"};
            for (String s : paths) {
                new File("elyousfi/"+s).createNewFile();
            }*/

            /*fs.copyFromLocalFile(new Path("elyousfi/TP1CPP.txt"),new Path("/BDCC1/CPP/TPs"));
            fs.copyFromLocalFile(new Path("elyousfi/TP2CPP.txt"),new Path("/BDCC1/CPP/TPs"));*/

            /*fs.copyFromLocalFile(new Path("elyousfi/TP1JAVA.txt"),new Path("/BDCC1/JAVA/TPs"));
            fs.copyFromLocalFile(new Path("elyousfi/TP2JAVA.txt"),new Path("/BDCC1/JAVA/TPs"));*/

            RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(new Path("/BDCC1"),true);
            while (filesList.hasNext()) {
                LocatedFileStatus next = filesList.next();
                System.out.println("filePath:" + next.getPath().toString());
            }

            // fs.delete(new Path("/BDCC1/CPP/TPs/TP1CPP.txt"),false);


            // fs.delete(new Path("/BDCC1/JAVA"),true);

            fs.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
