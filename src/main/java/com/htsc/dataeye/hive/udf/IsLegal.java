package com.htsc.dataeye.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class IsLegal {

	public static boolean isLeapYear(String data) throws ParseException {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date date = sdf.parse(data);
		Calendar calendarTmp = Calendar.getInstance();
		calendarTmp.setTime(date);

		int year = calendarTmp.get(Calendar.YEAR);
		if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
			return true;
		}
		return false;
	}


	public static boolean isValidDate(String s) {
		Date date1;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			date1 = dateFormat.parse(s);
			String date2 = dateFormat.format(date1);
			return date2.equals(s);
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static boolean dataIsLegal(String data) {
		if (data.trim().length() != 8) {

			return false;
		}
		return true;
	}

	public static boolean offsetIsLegal(int offset, int size) {
		if (offset >= 0) {
			if (offset <= size) {
				return true;
			}
			return false;
		} else {
			int a = -offset;
			if (a <= size) {
				return true;
			}
			return false;
		}

	}
}
