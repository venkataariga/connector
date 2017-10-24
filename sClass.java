package com.solace.cassandra;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class sClass {
	
	public static void main(String[] args) throws ParseException {
		
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String dateStr = dateFormat.format(cal.getTime());
		System.out.println(dateStr +"_AppData.csv");
	}

}
