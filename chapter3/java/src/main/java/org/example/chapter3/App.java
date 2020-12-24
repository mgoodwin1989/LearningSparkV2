package org.example.chapter3;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;



import java.util.Arrays;

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
                                    .appName("schemabuilder")
                                    .getOrCreate();
        String filename = args[0];
        StructType schema = DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("Id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("First", DataTypes.StringType, false),
                    DataTypes.createStructField("Last", DataTypes.StringType, false),
                    DataTypes.createStructField("Url", DataTypes.StringType, false),
                    DataTypes.createStructField("Published", DataTypes.StringType, false),
                    DataTypes.createStructField("Hits", DataTypes.IntegerType, false),
                    DataTypes.createStructField("Campaigns", DataTypes.createArrayType(DataTypes.StringType, false), false)));
        
        Dataset<Row> ds = sparkSession.read().schema(schema).json(filename);
        ds.show(false);
        ds.printSchema();
        System.out.println(ds.schema());
        ds.select(expr("Hits").multiply(2)).show(2);
        ds.select(col("Hits").multiply(2)).show(2);
        ds.select(expr("Hits * 2")).show(2);
        ds.withColumn("Big Hitters", (expr("Hits > 10000"))).show();
        sparkSession.stop();
    }
}
