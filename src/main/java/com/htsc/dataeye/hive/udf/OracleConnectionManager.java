package com.htsc.dataeye.hive.udf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class OracleConnectionManager {
	static String exchangeDB_url; 
	static String exchangeDB_username; 
	static String exchangeDB_pwd; 
	static String exchangeDB_table; 
	
	public static Connection getConnection() {
		Connection exchangeConn=null;
		try {
			Class.forName("oracle.jdbc.OracleDriver").newInstance();
			//先连配置库  获取信息再连接查询库
			Connection configConn = DriverManager.getConnection(CommonConfig.config_url, CommonConfig.config_user, CommonConfig.config_pwd);
			Statement statement=configConn.createStatement();
			//stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet rs=statement.executeQuery("select * from "+CommonConfig.config_queryTable +" where flag="+CommonConfig.config_type+"");
			while(rs.next()){
				exchangeDB_url=rs.getString(2);
				exchangeDB_username=rs.getString(3);
				exchangeDB_pwd=rs.getString(4);
				CommonConfig.exchange_table=rs.getString(5);
			}
			configConn.close();
			if(exchangeDB_url!=null){//获取信息正确
				exchangeConn= DriverManager.getConnection(exchangeDB_url,exchangeDB_username,exchangeDB_pwd);
			}
			return exchangeConn;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

}
