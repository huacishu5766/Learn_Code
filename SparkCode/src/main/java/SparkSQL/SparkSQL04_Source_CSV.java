package SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */
public class SparkSQL04_Source_CSV {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark01")
                .master("local[2]")
                .getOrCreate();

        //采用，分割的数据文件就是csv文件
        Dataset<Row> csv = sparkSession.read()
                .option("header","true")    //配置
                .option("sep","_")
                .csv("data/user.csv");

        //32 UUID String
        csv.write()
                .mode(SaveMode.Append)
                .option("header","true")
                .mode("overwrite")
                .csv("output");
        csv.show();


        sparkSession.close();
    }
}
