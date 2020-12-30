package org.example.chapter4;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;


import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



/**
 * Unit test for simple App.
 */
public class DataAccessTest 
{

    static Logger log = LogManager.getLogger(DataAccessTest.class);

    SparkSession sparkSession;
    Dataset<Row> ds;
    
    @BeforeEach
    public void setup() {
        sparkSession = SparkSession.builder()
            .master("local")
            .appName("SparkSQLExampleApp")
            .enableHiveSupport()
            .getOrCreate();

        String filename = "../../databricks-datasets/learning-spark-v2/flights/departuredelays.csv";
    
        // Note: if you don't set the schema then the date column will get "auto inferred" as an int and not a String
        ds = sparkSession.read()
                            .schema("date STRING, delay INT, distance INT,  origin STRING, destination STRING")
                            .format("csv")
                            //.option("inferSchema", true)
                            .option("header", true)
                            .load(filename);
        ds.createOrReplaceTempView("us_delay_flights_tbl");
    }

    /**
     * Rigorous Test :-)
     */
    @Test
    public void testSelectSFO_to_ORD_Delay()
    {
        Dataset<Row> ds1 = sparkSession.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC");
        assertEquals(55, ds1.count());
    }

    @Test
    public void testGroupByWithFunctionPackage() {
        Dataset<Row> ds1 = sparkSession.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC");
       
        // Note: since there is no year specified the date comes in as 1970
        // 2 different ways of showing the most frequently delayed months
        // first way using functions package
        Dataset<Row> ds2 = ds1.withColumn("date_time", functions.to_timestamp(ds1.col("date"), "MMddHHmm"));
        Row r = ds2.groupBy(functions.month(ds2.col("date_time")).as("month"))
                .count()
                .orderBy("month")
                .first();
        assertFalse(r.isNullAt(1));
        assertEquals(22, r.getLong(1)); //January count
        
    }

    @Test
    public void testGroupByWithSQL() {
        Row r = sparkSession.sql("SELECT date, delay, origin, destination, date_format(to_timestamp(date, 'MMddHHmm'), 'MMMM') AS month FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC")
            .groupBy("month")
            .count()
            .orderBy(desc("month"))
            .first();   
        assertFalse(r.isNullAt(1));
        assertEquals(16, r.getLong(1)); //March count   
    }

    @Test
    public void testSelectDistanceGreaterThan1000() {
        Dataset<Row> ds1 = sparkSession.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC");
        assertEquals(273571, ds1.count());
    }

    @Test
    public void testSQLWithCaseStatement() {
        Row r = sparkSession.sql("SELECT delay, origin, destination, " +
                "CASE " +
                    "WHEN delay > 360 THEN 'Very Long Delays' " +
                    "WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays' " +
                    "WHEN delay >= 60 AND delay < 120 THEN 'Short Delays' " +
                    "WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays' " +
                    "WHEN delay = 0 THEN 'No Delays' " +
                    "ELSE 'Early' " +
                "END AS Flight_Delays " +
                "FROM us_delay_flights_tbl " +
                "ORDER BY origin, delay DESC").first();
        assertFalse(r.isNullAt(0));
        assertEquals(333, r.getInt(0));
        assertEquals("ATL", r.getString(2));
    }

}
