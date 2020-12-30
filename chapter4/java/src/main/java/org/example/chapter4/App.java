package org.example.chapter4;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder()
                                    .master("local")
                                    .appName("SparkSQLExampleApp")
                                    .getOrCreate();
        String filename = "../databricks-datasets/learning-spark-v2/flights/departuredelays.csv";
        
        // Note: if you don't set the schema then the date column will get "auto inferred" as an int and not a String
        Dataset<Row> ds = sparkSession.read()
                            .schema("date STRING, delay INT, distance INT,  origin STRING, destination STRING")
                            .format("csv")
                            //.option("inferSchema", true)
                            .option("header", true)
                            .load(filename);
        ds.createOrReplaceTempView("us_delay_flights_tbl");
        


        sparkSession.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC").show(10);
        Dataset<Row> ds1 = sparkSession.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC");
        ds1.show(10);
        
        // Note: since there is no year specified the date comes in as 1970
        // 2 different ways of showing the most frequently delayed months
        // first way using functions package
        Dataset<Row> ds2 = ds1.withColumn("date_time", functions.to_timestamp(ds1.col("date"), "MMddHHmm"));
        //ds2.show(10);
        ds2.groupBy(functions.month(ds2.col("date_time"))).count().show(10);
        
        // second way is within sql
        sparkSession.sql("SELECT date, delay, origin, destination, date_format(to_timestamp(date, 'MMddHHmm'), 'MMMM') AS month FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC")
            .groupBy("month")
            .count()
            .show(10);
        
        sparkSession.sql("SELECT delay, origin, destination, " +
                "CASE " +
                    "WHEN delay > 360 THEN 'Very Long Delays' " +
                    "WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays' " +
                    "WHEN delay >= 60 AND delay < 120 THEN 'Short Delays' " +
                    "WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays' " +
                    "WHEN delay = 0 THEN 'No Delays' " +
                    "ELSE 'Early' " +
                "END AS Flight_Delays " +
                "FROM us_delay_flights_tbl " +
                "ORDER BY origin, delay DESC").show(10);
       
        
    }

   
}
