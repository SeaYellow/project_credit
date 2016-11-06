package com.merit.ut;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2016/9/19.
 */
public class Main {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final Date date = sdf.parse("2016-08-19 22:51:32.0");
        System.out.println(date.getTime());

        String str = "aa";
        System.out.println(str.split(" ")[0]);
    }
}
