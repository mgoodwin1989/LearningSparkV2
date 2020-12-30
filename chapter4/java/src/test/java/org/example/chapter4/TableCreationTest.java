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

public class TableCreationTest {

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

        sparkSession.sql("DROP TABLE IF EXISTS managed_us_delay_flights_tbl");
        sparkSession.sql("DROP TABLE IF EXISTS us_delay_flights_tbl");
    }
    
    @Test
    public void testCreateManagedTable() {
        sparkSession.sql("CREATE TABLE managed_us_delay_flights_tbl (date String, delay INT, distance INT, origin String, destination String)");

    }

    @Test
    public void testCreateManagedTableDataFrameAPI() {
        String filename = "../../databricks-datasets/learning-spark-v2/flights/departuredelays.csv";
        String schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING";
        Dataset<Row> ds = sparkSession.read().schema(schema).csv(filename);
        ds.write().saveAsTable("managed_us_delay_flights_tbl");
    }

    @Test
    public void testCreateUnManagedTable() {
        sparkSession.sql("CREATE TABLE us_delay_flights_tbl (date String, delay INT, distance INT, origin String, destination String) USING csv OPTIONS (PATH '../../databricks-datasets/learning-spark-v2/flights/departuredelays.csv')");
    }

    @Test
    public void testCreateUnmanagedTableDataFrameAPI() {
        String filename = "../../databricks-datasets/learning-spark-v2/flights/departuredelays.csv";
        String schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING";
        Dataset<Row> ds = sparkSession.read().schema(schema).csv(filename);
        String path = "us_flight_delay";
        ds.write().option("path", path)
        .saveAsTable("us_delay_flights_tbl");
    }

    @Test
    public void testCreateGlobalViewDataFrameAPI() {
        sparkSession.sql("CREATE TABLE managed_us_delay_flights_tbl (date String, delay INT, distance INT, origin String, destination String)");
        Dataset<Row> ds = sparkSession.sql("SELECT date, delay, origin, destination FROM managed_us_delay_flights_tbl WHERE origin = 'SFO'");
        ds.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view");
    }

    @Test
    public void testCreateTempViewDataFrameAPI() {
        sparkSession.sql("CREATE TABLE managed_us_delay_flights_tbl (date String, delay INT, distance INT, origin String, destination String)");
        Dataset<Row> ds = sparkSession.sql("SELECT date, delay, origin, destination FROM managed_us_delay_flights_tbl WHERE origin = 'JFK'");
        ds.createOrReplaceTempView("us_origin_airport_JFK_tmp_view");
    }
}
