package com.htsc.dataeye.hive.udf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GetTradeDay extends UDF {

	public Text evaluate(Text data, IntWritable offset, Text exchange_type) {

		if (!IsLegal.isValidDate(data.toString())) {
			return new Text(CommonConfig.errorMsg);
		}

		Connection conn = OracleConnectionManager.getConnection();
		if (conn == null) {
			return new Text(CommonConfig.errorMsg);
		}

		Long inputDateLong = Long.parseLong(data.toString());
		PreparedStatement ps;
		try {
			int index = offset.get();
			if (index == 0) {
				return new Text(data.toString());
			}
			String sql = "";
			if (index < 0) {
				sql = "select exch_date from ( select distinct exch_date from "
						+ CommonConfig.exchange_table
						+ " where exch_date < ? and EXCHANGE_TYPE = "
						+ exchange_type.toString()
						+ " order by exch_date desc ) where  rownum <= ?";
			} else {
				sql = "select exch_date from ( select distinct exch_date from "
						+ CommonConfig.exchange_table
						+ " where exch_date > ? and EXCHANGE_TYPE = "
						+ exchange_type.toString()
						+ " order by exch_date asc ) where  rownum <= ?";
			}

			ps = conn.prepareStatement(sql);
			ps.setLong(1, inputDateLong);
			ps.setInt(2, Math.abs(offset.get()));
			ResultSet rs = ps.executeQuery();
			String tmp = "";
			int fetchCount = 0;// 鏉╂柨娲栭弫鐗堝祦缂佹挻鐏夐幀缁樻殶
			while (rs.next()) {
				fetchCount++;
				tmp = rs.getString(1);
			}
			if (fetchCount < Math.abs(index)) {// 閼惧嘲褰囬惃鍕唶瑜版洘鏆熷▽鈩冩箒鏉堟儳鍩岄崑蹇曅╅柌锟�				return new Text(CommonConfig.errorMsg);
			}
			return new Text(tmp);
		} catch (SQLException e) {
			e.printStackTrace();
			return new Text(CommonConfig.errorMsg);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Text evaluate(IntWritable offset, Text exchange_type) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");//鐠佸墽鐤嗛弮銉︽埂閺嶇厧绱�
		String inputDate=df.format(new Date());
		return evaluate(new Text(inputDate),offset,exchange_type);
	}
}