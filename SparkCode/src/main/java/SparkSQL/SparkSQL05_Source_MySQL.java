package SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */
public class SparkSQL05_Source_MySQL {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark01")
                .master("local[2]")
                .getOrCreate();

        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123456");


        Dataset<Row> jdbc = sparkSession.read().jdbc("jdbc:mysql://hadoop102:3306/gmall?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true", "test_info", properties);

    //        json.write()
    //          // 写出模式针对于表格追加覆盖
    //            .mode(SaveMode.Append)
    //            .jdbc("jdbc:mysql://hadoop102:3306","gmall.testInfo",properties);

        jdbc.show();

        sparkSession.close();
    }
}
