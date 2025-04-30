package com.home.demo.raw;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class EnrichPublisher {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Refined To Enrich publisher")
                .master("local[*]")
                .getOrCreate();
    }
}
