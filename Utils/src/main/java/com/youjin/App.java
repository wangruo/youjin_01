package com.youjin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{

//    private static boolean checkHdfsDirectory(String path) throws IOException {
//        Configuration conf = new Configuration();
//        FileSystem file = FileSystem.get(conf);
//        Path hdfsPath = new Path(path);
//        if(file.exists(hdfsPath) && file.isDirectory(hdfsPath))
//        {
//            return true;
//        }
//        System.err.println("The hdfs path [" + path + "] can't find");
//        return false;
//    }


//    void AddSubDir(FileSystem fs, Job job, Path path) throws IOException {
//        if(fs.exists(path) && fs.isDirectory(path))
//        {
//            FileInputFormat.addInputPath(job, path);
//            System.out.println(path);
//            FileStatus[] fileStatuses = fs.listStatus(path);
//            for(int i = 0; i < fileStatuses.length; i++) {
//                Path subPath = fileStatuses[i].getPath();
//                if (fs.isDirectory(subPath)) {
//                    AddSubDir(fs, job, subPath);
//                }
//            }
//        }
//    }

//    private static boolean checkDirectory(String path)
//    {
//        File file = new File(path);
//        if(file.exists() && file.isDirectory())
//        {
//            return true;
//        }
//        System.err.println("The local path [" + path + "] can't find");
//        return false;
//    }

    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    }
}
