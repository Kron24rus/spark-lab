package com.fireway.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.window;

public class LogsAnalyzer {

    private static final SimpleDateFormat SIMPLE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
    private SparkSession sparkSession;
    private JavaSparkContext sparkContext;

    public static void main(String[] args) {
        String input = "hdfs://" + args[0] + ":9000" + args[1];
        String output = "hdfs://" + args[0] + ":9000";
        new LogsAnalyzer().analyze(input, output);
    }

    public void analyze(String input, String output) {
        SparkConf conf = new SparkConf().setAppName("Logs Analyzer");
        sparkContext = new JavaSparkContext(conf);
        sparkSession = SparkSession
                .builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        errorRequestCount(input, output);
        timeRequestCount(input, output);
        windowRequestCount(input, output);
    }

    private void errorRequestCount(String inputFileName, String outputFileName) {
        JavaRDD<BaseLogRequest> requests = sparkContext
                .textFile(inputFileName)
                .map(BaseLogRequest.parseRequest)
                .filter(x -> x != null);

        JavaPairRDD<String, Integer> serverErrorList = requests
                .filter(x -> x.getReplyCode() >= 500 && x.getReplyCode() < 600)
                .mapToPair(x -> new Tuple2<>(x.getUrl() + " " + x.getReplyCode(), 1));

        serverErrorList
                .reduceByKey((i1,i2) -> i1+i2)
                .coalesce(1)
                .saveAsTextFile(outputFileName + "/task1");
    }

    private void timeRequestCount(String inputFile, String outputFile) {
        JavaRDD<BaseLogRequest> requests = sparkContext
                .textFile(inputFile)
                .map(BaseLogRequest.parseRequest)
                .filter(x -> x != null);

        JavaPairRDD<String, Integer> requestCountByDate = requests
                .mapToPair(x -> new Tuple2<>(SIMPLE_FORMATTER.format(x.getDate()) + " " + x.getMethod() + " "  + x.getReplyCode(), 1));

        requestCountByDate
                .reduceByKey((i1,i2) -> i1+i2)
                .filter((x) -> x._2 > 10)
                .sortByKey()
                .coalesce(1)
                .saveAsTextFile(outputFile + "/task2");
    }

    private void windowRequestCount(String inputFile, String outputFile) {
        JavaRDD<BaseLogRequest> requests = sparkContext
                .textFile(inputFile)
                .map(BaseLogRequest.parseRequest)
                .filter(x -> x != null && x.getReplyCode() >= 400 && x.getReplyCode() < 600);


        Dataset<Row> errorRequestsCount = sparkSession.createDataFrame(requests, BaseLogRequest.class);

        errorRequestsCount
                .groupBy(window(errorRequestsCount.col("date"), "1 week", "1 day"))
                .agg(count("replyCode").as("count"))
                .select("window.start", "window.end", "count")
                .orderBy("start")
                .javaRDD()
                .map(r -> new Tuple3<>(
                        SIMPLE_FORMATTER.format(r.getTimestamp(0)),
                        SIMPLE_FORMATTER.format(r.getTimestamp(1)),
                        r.getLong(2)))
                .coalesce(1)
                .saveAsTextFile(outputFile + "/task3");
    }
}
