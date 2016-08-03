package com.htsc.dataeye.hive.udf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Test {
	public static Text evaluate(Text data, IntWritable offset, Text exchange_type) {

		if (!IsLegal.isValidDate(data.toString())) {
			return new Text(CommonConfig.errorMsg);
		}
		Calendar today = Calendar.getInstance();
		SimpleDateFormat year = new SimpleDateFormat("yyyy");
		List<String> list=HDFSUtils.getDataFromLocal(year.format(today.getTime()), exchange_type.toString());
		int s=0;
		if(list.size()==0){
			return new Text("sss");
		}
		for (int i = 0; i < list.size(); i++) {
			if(Integer.parseInt(list.get(i))<=Integer.parseInt(data.toString())){
				s=i;
			}	
		}
		if (list.get(s).equals(data.toString())) {
			return new Text(list.get(s+ offset.get()));
		}else{
			if(offset.get()==0){
				return new Text("00000000");
			}else if(offset.get()>0){
				return new Text(list.get(s+ offset.get()));
			}else{
				s=s+1;
				return new Text(list.get(s+ offset.get()));
			}
			
		}
	
	}
	
	public static Text evaluate(IntWritable offset, Text exchange_type) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		String inputDate=df.format(new Date());
		return evaluate(new Text(inputDate),offset,exchange_type);
	}

	public static void main(String[] args) {

		System.out.println(evaluate( new IntWritable(0),new Text("1")));
		System.out.println(evaluate(new IntWritable(-1),new Text("1")));
		System.out.println(evaluate(new Text("20160803"), new IntWritable(1),new Text("1")));
		System.out.println(evaluate(new Text("20160803"), new IntWritable(3),new Text("1")));
	}

}
