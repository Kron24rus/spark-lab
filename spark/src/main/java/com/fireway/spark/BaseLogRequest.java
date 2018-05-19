package com.fireway.spark;

import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class BaseLogRequest implements Serializable {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
    private String url;
    private Date date;
    private String method;
    private Integer replyCode;

    public String getUrl() {
        return url;
    }

    public Date getDate() {
        return date;
    }

    public String getMethod() {
        return method;
    }

    public Integer getReplyCode() {
        return replyCode;
    }

    public static BaseLogRequest parseRequest(String s) {
        try {
            String[] blocks = s.split("\"");
            //URL & Date
            String[] dateAndUrl = blocks[0].split(" ");
            String url = dateAndUrl[0];
            LocalDateTime date = LocalDateTime.parse(dateAndUrl[3].substring(1), DATE_TIME_FORMATTER);
            //Method
            String method = blocks[1].split(" ")[0];
            //replyCode
            int replyCode = Integer.parseInt(blocks[2].split(" ")[1]);
            return new BaseLogRequest(url, new Date(date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()), method,replyCode);
        } catch (Exception e) {
            System.out.println("Error parsing string: " + s + " Message: " + e.getMessage());
        }

        return null;
    }

    public static Function<String, BaseLogRequest> parseRequest = s -> BaseLogRequest.parseRequest(s);

    public BaseLogRequest() {

    }

    private BaseLogRequest(String url, Date date, String method, int replyCode) {
        this.url = url;
        this.date = date;
        this.method = method;
        this.replyCode = replyCode;
    }

    @Override
    public String toString() {
        return "PARSED LOG:\n" +
                "url\t" + url + "\n" +
                "date\t" + date.toString() + "\n" +
                "method\t" + method + "\n" +
                "replyCode\t" + replyCode + "\n";
    }
}
