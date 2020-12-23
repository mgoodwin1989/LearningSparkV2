package org.example.chapter2;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class JavaWordCount {

    public static void main(String[] args) {
        String filename = args[0];
        colorCount(filename);
    }   

    public static void colorCount(String filename) {
        SparkSession sparkSession = SparkSession.builder()
                                .master("local")
                                .appName("name")
                                .getOrCreate();

        Dataset<Row> ds = sparkSession.read().format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(filename);
        Dataset<Row> count_ds = ds.select("State", "Color","Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy(desc("sum(Count)"));
        count_ds.show(60, false);
        Dataset<Row> ca_count_ds = 
                        ds.select("State", "Color","Count")
                        .where("State == 'CA'")
                        .groupBy("State", "Color")
                        .sum("Count")
                        .orderBy(desc("sum(Count)"));
        ca_count_ds.show(10, false);
        sparkSession.stop();
    }
}
