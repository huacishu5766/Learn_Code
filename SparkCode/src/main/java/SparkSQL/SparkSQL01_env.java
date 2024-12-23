package SparkSQL;

import org.apache.spark.sql.SparkSession;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */
public class SparkSQL01_env {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("SparkSQL01_env")
                .getOrCreate();


        sparkSession.close();
    }
}
