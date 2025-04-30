package com.home.demo.raw;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RefinedPublisher {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Raw To refined publisher")
                .master("local[*]")
                .getOrCreate();

        StructType orderSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("ORDER_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("ORDER_DATETIME", DataTypes.StringType, false),
                DataTypes.createStructField("CUSTOMER_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("ORDER_STATUS", DataTypes.StringType, false),
                DataTypes.createStructField("STORE_ID", DataTypes.IntegerType, false)
        });

        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .schema(orderSchema)
                .csv("C:\\interview-workspace\\DATASETS\\raw\\orders.csv");


        //Dataset<Row> ordersWithTs = orders.withColumn("ORDER_TIMESTAMP", functions.to_timestamp(orders.col("ORDER_DATETIME"), "dd-MMM-yy kk.mm.ss.SS"));
        orders = orders.select(orders.col("ORDER_ID"),
                functions.to_timestamp(orders.col("ORDER_DATETIME"), "dd-MMM-yy kk.mm.ss.SS").alias("ORDER_TIMESTAMP"),
                orders.col("CUSTOMER_ID"),
                orders.col("ORDER_STATUS"),
                orders.col("STORE_ID")
        );
        System.out.println( orders.count());
        orders.printSchema();
        orders.show();
        Dataset<Row> ordersFiltered = orders.filter(col("ORDER_STATUS").equalTo("COMPLETE"));
        ordersFiltered.show();
        System.out.println( ordersFiltered.count());

        StructType storeSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("STORE_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("STORE_NAME", DataTypes.StringType, false),
                DataTypes.createStructField("WEB_ADDRESS", DataTypes.StringType, false),
                DataTypes.createStructField("LATITUDE", DataTypes.DoubleType, false),
                DataTypes.createStructField("LONGITUDE", DataTypes.DoubleType, false)
        });

        Dataset<Row> stores = spark.read()
                .option("header", "true")
                .schema(storeSchema)
                .csv("C:\\interview-workspace\\DATASETS\\raw\\stores.csv");
        stores.show(5);

        Dataset<Row> orderStoreJoin = ordersFiltered.join(stores, ordersFiltered.col("STORE_ID").equalTo(stores.col("STORE_ID")), "left");
        orders = orderStoreJoin.select(col("ORDER_ID"),col("ORDER_TIMESTAMP"),col("CUSTOMER_ID"),col("STORE_NAME"));
        orders.write().mode("overwrite").parquet("C:\\interview-workspace\\DATASETS\\refined\\orders");


        StructType lineItemSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("ORDER_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("LINE_ITEM_ID", DataTypes.StringType, false),
                DataTypes.createStructField("PRODUCT_ID", DataTypes.StringType, false),
                DataTypes.createStructField("UNIT_PRICE", DataTypes.DoubleType, false),
                DataTypes.createStructField("QUANTITY", DataTypes.IntegerType, false)
        });

        Dataset<Row> orderItems = spark.read()
                .option("header", "true")
                .schema(lineItemSchema)
                .csv("C:\\interview-workspace\\DATASETS\\raw\\order_items.csv");

        orderItems = orderItems.drop(col("LINE_ITEM_ID"));
        orderItems.show();
        orderItems.write().mode("overwrite").parquet("C:\\interview-workspace\\DATASETS\\refined\\order_items");


        StructType productSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("PRODUCT_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("PRODUCT_NAME", DataTypes.StringType, false),
                DataTypes.createStructField("UNIT_PRICE", DataTypes.DoubleType, false)
        });

        Dataset<Row> products = spark.read()
                .option("header", "true")
                .schema(productSchema)
                .csv("C:\\interview-workspace\\DATASETS\\raw\\products.csv");
        products.show();
        products.write().mode("overwrite").parquet("C:\\interview-workspace\\DATASETS\\refined\\products");

        StructType customerSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("CUSTOMER_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("FULL_NAME", DataTypes.StringType, false),
                DataTypes.createStructField("EMAIL_ADDRESS", DataTypes.StringType, false)
        });

        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .schema(customerSchema)
                .csv("C:\\interview-workspace\\DATASETS\\raw\\customers.csv");
        customers.show();
        customers.write().mode("overwrite").parquet("C:\\interview-workspace\\DATASETS\\refined\\customers");
    }
}
