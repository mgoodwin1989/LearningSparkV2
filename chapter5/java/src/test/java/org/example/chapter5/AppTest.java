package org.example.chapter5;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import scala.collection.JavaConverters;
import scala.collection.Seq;


/**
 * Unit test for simple App.
 */
public class AppTest implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    SparkSession spark;

    @BeforeEach
    public void setup() {
        spark = SparkSession.builder()
                .master("local")
                .appName("Chapter5Examples")
                .enableHiveSupport()
                .getOrCreate();
    }

    @Test
    public void testUdf() {
        //spark.udf().register("cubed", cubed, DataTypes.LongType);
        spark.udf().register("cubed", (Long l) -> l*l*l, DataTypes.LongType);
        spark.range(1, 9).createOrReplaceTempView("udf_test");
        spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show();
    }

    @Test
    public void testPlusThree() {
        Dataset<Row> ds = loadStudentsJsonFile();
        ds.show();
        ds.createOrReplaceTempView("vw_students");

        spark.udf().register("plusThree", (Seq<Integer> seq) -> {
            return JavaConverters.seqAsJavaList(seq)
                                    .stream()
                                    .map(i -> i+3)
                                    .collect(Collectors.toList());
        }, DataTypes.createArrayType(DataTypes.IntegerType, false));

        Dataset<Row> ds2 = spark.sql("SELECT id, name, plusThree(grades) FROM vw_students");
        ds2.show();
        
    }

    @Test
    public void testTransform() {
        Dataset<Row> ds = loadStudentsJsonFile();
        ds.show();
        ds.createOrReplaceTempView("vw_students");

        Dataset<Row> ds2 = spark.sql("SELECT id, name, grades, transform(grades, i -> i+3) as plusThree FROM vw_students");
        ds2.show();
    }

    @Test
    @Disabled
    //Note: reduce does not seem to be a part of this spark release
    public void testReduce() {
        Dataset<Row> ds = loadStudentsJsonFile();
        ds.createOrReplaceTempView("vw_students");
        Dataset<Row> ds2 = spark.sql("SELECT id, name, grades, reduce(grades, 0, (i, acc) -> i + acc, acc -> (acc div size(grades))) as avgGrade FROM vw_students");
        ds2.show();
    }

    @Test
    public void testCasting() {
        Dataset<Row> ds = loadDepartureDelays();
        ds = ds.withColumn("delay", expr("CAST(delay as INT) as delay"))
                .withColumn("distance", expr("CAST(distance as INT) as distance"));
        ds.createOrReplaceTempView("departureDelays");
        spark.sql("SELECT * from departureDelays").show(10);
    }

    @Test
    public void testUnion() {
        Dataset<Row> ds = loadDepartureDelays();
        ds = ds.withColumn("delay", expr("CAST(delay as INT) as delay"))
                .withColumn("distance", expr("CAST(distance as INT) as distance"));
        ds.createOrReplaceTempView("departureDelays");
        
        // Filters dataset down to 3 records
        Dataset<Row> foo = (ds.filter(expr("origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0")));
        foo.createOrReplaceTempView("foo");
        assertEquals(3, foo.count());
        
        Dataset<Row> bar = ds.union(foo).filter(expr("origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0"));
        bar.createOrReplaceTempView("bar");
        assertEquals(6, bar.count());
    }

    @Test
    public void testJoin() {
        Dataset<Row> airports = loadAirports();
        Dataset<Row> ds = loadDepartureDelays();
        ds = ds.withColumn("delay", expr("CAST(delay as INT) as delay"))
                .withColumn("distance", expr("CAST(distance as INT) as distance"));
        ds.createOrReplaceTempView("departureDelays");
        
        // Filters dataset down to 3 records
        Dataset<Row> foo = (ds.filter(expr("origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0")));
        foo.createOrReplaceTempView("foo");
        foo.join(airports.as("air"), col("IATA").equalTo(col("origin")), "inner")
            .select("City", "State", "date", "delay", "distance", "destination").show();
    }

    @Test
    public void testWindowing() {
        Dataset<Row> ds = loadDepartureDelays();
        ds = ds.withColumn("delay", expr("CAST(delay as INT) as delay"))
                .withColumn("distance", expr("CAST(distance as INT) as distance"));
        ds.createOrReplaceTempView("departureDelays");

        spark.sql("DROP TABLE IF EXISTS departureDelaysWindow");
        spark.sql("CREATE TABLE departureDelaysWindow AS SELECT origin, destination, SUM(delay) as TotalDelays FROM departureDelays WHERE origin IN ('SEA', 'SFO', 'JFK') AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') GROUP BY origin, destination;");
        //spark.sql("SELECT * FROM departureDelaysWindow").show();
        spark.sql("SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t WHERE rank <= 3").show();
        
    }

    @Test
    public void testDenseRankDataFrameAPI() {
        Dataset<Row> ds = loadDepartureDelays();
        ds = ds.withColumn("delay", expr("CAST(delay as INT) as delay"))
                .withColumn("distance", expr("CAST(distance as INT) as distance"))
                //.withColumn("TotalDelays", sum("delay"));
                //.withColumn("rank", dense_rank().over(Window.partitionBy("origin").orderBy(sum("delay"))))
                .where("origin IN ('SEA', 'SFO', 'JFK') AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')");
        // ds = ds.withColumn("TotalDelays", sum("delay"))
        //         .withColumn("rank", dense_rank().over(Window.partitionBy("origin").orderBy("TotalDelays")));
        ds.createOrReplaceTempView("departureDelaysRank");
        ds.show(10);
    }

    private Dataset<Row> loadStudentsJsonFile() {
        String path = "../data/students.json";

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("grades", DataTypes.createArrayType(DataTypes.IntegerType, false), false)
        ));

        Dataset<Row> ds = spark.read().schema(schema).json(path);
        return ds;
    }

    private Dataset<Row> loadAirports() {
        String path = "../data/airport-codes-na.txt";
        Dataset<Row> ds = spark.read()
                            .format("csv")
                            .option("header", true)
                            .option("inferSchema", true)
                            .option("sep", "\t")
                            .load(path);
        return ds;
    }

    private Dataset<Row> loadDepartureDelays() {
        String path = "../data/departuredelays.csv";
        Dataset<Row> ds = spark.read()
                            .format("csv")
                            .option("header", true)
                            .load(path);
        return ds;
    }
}
