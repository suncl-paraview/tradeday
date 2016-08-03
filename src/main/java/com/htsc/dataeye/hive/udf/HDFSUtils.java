package com.htsc.dataeye.hive.udf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtils {

	public static List<String> getDataFromHDFS(String dateString,String type) {
		List<String> exchangeDateList = new ArrayList<String>();
		String dataPath = "hdfs://nameservice1/udf/exchange.txt";
		try {
			FileSystem fileSystem = FileSystem.get(new Configuration());
			Path path = new Path(dataPath);
			InputStream in = fileSystem.open(path);
			BufferedReader buff = new BufferedReader(new InputStreamReader(in));
			String str = null;
			while ((str = buff.readLine()) != null) {
				String[] lineContent=str.split(",");
				String firstPartDate = lineContent[0];
				String year = firstPartDate.substring(0, 4);
				if (year.compareTo(dateString) < 0) {
					continue;
				}
				if (year.compareTo(dateString) > 0) {
					break;
				}
				if(lineContent[1].equalsIgnoreCase(type)){
					exchangeDateList.add(firstPartDate);
				}
			}
			buff.close();
			in.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		return exchangeDateList;
	}

	public static List<String> getDataFromLocal(String yearDate,String type) {
		List<String> exchangeDateList = new ArrayList<String>();

		try {
		 InputStream in =GetLocalTradeDay.class.getClassLoader().getResourceAsStream("exchange.txt");
	   	 BufferedReader buff=new BufferedReader(new InputStreamReader(in));  
	        String str=null;       
			while ((str = buff.readLine()) != null) {
				String[] lineContent=str.split(",");
				String firstPartDate = lineContent[0];
				String year = firstPartDate.substring(0, 4);
				if (year.compareTo(yearDate) < 0) {
					continue;
				}
				if (year.compareTo(yearDate) > 0) {
					break;
				}
				
				if(lineContent[1].equalsIgnoreCase(type)){
					exchangeDateList.add(firstPartDate);
				}
			}
			buff.close();
			in.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return exchangeDateList;
	}

	public static void main(String[] args) {
		List<String> exchangeDateList = HDFSUtils.getDataFromLocal("20160615".substring(0,4),"1");
		for (String tmp : exchangeDateList) {
			System.out.println(tmp);
		}
	}
	/*public static void main(String[] args) {
		 InputStream is =GetLocalTradeDay.class.getClassLoader().getResourceAsStream("exchange.txt");
	   	 BufferedReader br=new BufferedReader(new InputStreamReader(is));  
	        String s;
	        try {
				while((s=br.readLine())!=null){
				 System.out.println(s);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}


		}*/

}
