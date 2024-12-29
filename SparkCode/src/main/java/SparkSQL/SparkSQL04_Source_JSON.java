package SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */
public class SparkSQL04_Source_JSON {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark01")
                .master("local[2]")
                .getOrCreate();

        //JSON : JavaScript Object Notation
        //      对象 ： {}
        //      数组 ： []
        //      JSON文件： 整个文件的数据格式符合JSON格式，不是一行数据符合JSON格式
        //      SparkSQL读取JSON文件时，是 Spark Core RDD的封装，底层是Hadoop的按行读取，
        //      SparkSQL只需要保证JSON文件中一行数据符合JSON格式，无需整个文件符合
        //      默认会按照JSON对象的格式进行解析，如果JSON文件不是一行数据符合JSON格式，则需要指定schema进行解析
        Dataset<Row> json = sparkSession.read()
                .json("data/user.json");

        json.write().json("output");
        json.show();



        sparkSession.close();
    }
}
