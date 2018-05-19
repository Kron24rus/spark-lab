package com.fireway.spark;

import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class BaseLogRequest implements Serializable {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
    private static final int HOST_INDEX = 0;
    private static final int DATE_INDEX = 3;
    private static final int METHOD_INDEX = 5;
    private static final int PATH_TO_FILE_INDEX = 6;
    private static final int PROTOCOL_INDEX = 7;
    private static final int REPLY_CODE_INDEX = 8;
    private static final int REPLY_BYTES_INDEX = 9;
    private String url;
    private Date date;
    private String method;
    private String resourcePath;
    private String protocol;
    private Integer replyCode;
    private Integer contentLength;

    public String getUrl() {
        return url;
    }

    public Date getDate() {
        return date;
    }

    public String getMethod() {
        return method;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public String getProtocol() {
        return protocol;
    }

    public Integer getReplyCode() {
        return replyCode;
    }

    public Integer getContentLength() {
        return contentLength;
    }

    public static BaseLogRequest parseRequest(String s) {
        try {
            s = s.replaceAll("[\",\\[,\\]]", "").trim().replaceAll("\\s{2,}", " ");
            String[] parts = s.split(" ");

            String host = parts[HOST_INDEX];
            LocalDateTime dateTime = LocalDateTime.parse(parts[DATE_INDEX], DATE_TIME_FORMATTER);
            String method = parts[METHOD_INDEX];
            String pathToFile = parts[PATH_TO_FILE_INDEX];
            String protocol = parts[PROTOCOL_INDEX];
            int replyCode = Integer.parseInt(parts[REPLY_CODE_INDEX]);
            int bytesInReply = 0;
            if (!parts[REPLY_BYTES_INDEX].equals("-")) {
                bytesInReply = Integer.parseInt(parts[REPLY_BYTES_INDEX]);
            }
            return new BaseLogRequest(host, new Date(dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()), method, pathToFile, protocol, replyCode, bytesInReply);
        } catch (Exception ex) {
            System.out.println("Error parsing string: " + s + " Message: " + ex.getMessage());
        }

        return null;
    }

    public static Function<String, BaseLogRequest> parseRequest = s -> BaseLogRequest.parseRequest(s);

    public BaseLogRequest() {

    }

    private BaseLogRequest(String host, Date dateTime, String method, String pathToFile, String protocol, int replyCode, int bytesInReply) {
        this.url = host;
        this.date = dateTime;
        this.method = method;
        this.resourcePath = pathToFile;
        this.protocol = protocol;
        this.replyCode = replyCode;
        this.contentLength = bytesInReply;
    }

    @Override
    public String toString() {
        return "PARSED LOG:\n" +
                "url\t" + url + "\n" +
                "date\t" + date + "\n" +
                "method\t" + method + "\n" +
                "resourcePath\t" + resourcePath + "\n" +
                "protocol\t" + protocol + "\n" +
                "replyCode\t" + replyCode + "\n" +
                "contentLength\t" + contentLength + "\n";
    }
}
