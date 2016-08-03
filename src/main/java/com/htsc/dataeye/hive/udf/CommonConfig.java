package com.htsc.dataeye.hive.udf;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


public class CommonConfig {

	public static String config_url = "jdbc:oracle:thin:@168.8.66.119:1521:reora1";
    public static String config_user = "datastage";
    public static String config_pwd = "Dw2011reora";
    public static String config_queryTable = "datastage.hive_udf_config";
    public static String config_type = "001";
    
    
    public static String errorMsg="00000000";
    public static DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    
    public static String exchange_table = "realhq.conexchangedate";  
	
	
}
