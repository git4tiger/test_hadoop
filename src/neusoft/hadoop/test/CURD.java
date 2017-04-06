package neusoft.hadoop.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class CURD {
	//public static String HDFS_PATH="hdfs://10.176.8.41:9110";
	public static String HDFS_PATH="hdfs://10.176.12.229:9000";
	public static void main(String[] args) throws Exception {
		//read(HDFS_PATH);
		mkdir(HDFS_PATH);
		//upload(HDFS_PATH);
		//download(HDFS_PATH);
		//delete(HDFS_PATH);
	}
	
	
	public static void read(String HDFS_PATH) throws Exception{
		String filePath="/test_can_delete/core-site.xml";
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url=new URL(HDFS_PATH+filePath);
		InputStream is=url.openStream();
		IOUtils.copyBytes(is, System.out, 1024, true);
	}
	
	
	public static void mkdir(String HDFS_PATH) throws Exception{
		
		String mkdir="/dir_for_lyz";
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI(HDFS_PATH),conf);
		fs.mkdirs(new Path(mkdir));
	}
	
	
	public static void upload(String HDFS_PATH) throws Exception{
		
		String fileName="/dir_for_luyizhi/收费电子书列表.xls";
		String filePath="G:\\文档\\收费电子书列表.xls";
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI(HDFS_PATH),conf);
		FSDataOutputStream out=fs.create(new Path(fileName), true);
		
		IOUtils.copyBytes(new FileInputStream(new File(filePath)), out, 1024, true);
	}
	
	
	public static void download(String HDFS_PATH) throws Exception{
		
		String fileName="/mydir2/�շѵ������б�.xls";
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI(HDFS_PATH),conf);
		FSDataInputStream in=fs.open(new Path(fileName));
		
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
	

	public static void delete(String HDFS_PATH) throws Exception{
		
		String fileName="/mydir2";
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI(HDFS_PATH),conf);
		fs.delete(new Path(fileName),true);
	}
	
	

}
