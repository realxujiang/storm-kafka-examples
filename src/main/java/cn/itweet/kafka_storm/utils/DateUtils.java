package cn.itweet.kafka_storm.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Andrew on 2015-5-19.
 */
public class DateUtils {
	public static final String C_DATE_PATTON_DEFAULT = "yyyy-MM-dd";
	
	public static boolean isValidDate(String createDate,String startDate) {
		try {
			SimpleDateFormat format = new SimpleDateFormat(C_DATE_PATTON_DEFAULT);
			Date cdate = format.parse(createDate);
			Date sdate = format.parse(startDate);
			if(cdate.getTime()>=sdate.getTime()) {
				return true;
			}else {
				return false;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(isValidDate("2015-01-01", "2014-04-01")) {
			System.out.println("true");
		}else {
			System.out.println("false");
		}
	}

}
