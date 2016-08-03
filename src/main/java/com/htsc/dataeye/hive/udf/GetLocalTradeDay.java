package com.htsc.dataeye.hive.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GetLocalTradeDay extends UDF {
	

	public Text evaluate(Text data, IntWritable offset, Text exchange_type) {

		if (!IsLegal.isValidDate(data.toString())) {
			return new Text(CommonConfig.errorMsg);
		}
		Calendar today = Calendar.getInstance();
		SimpleDateFormat year = new SimpleDateFormat("yyyy");
		List<String> list=HDFSUtils.getDataFromLocal(year.format(today.getTime()), exchange_type.toString());
		
		if(list.size()==0){
			return new Text("00000000");
		}
		int s=0;
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
	
	public Text evaluate(IntWritable offset, Text exchange_type) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");//璁剧疆鏃ユ湡鏍煎紡
		String inputDate=df.format(new Date());
		return evaluate(new Text(inputDate),offset,exchange_type);
	}
}