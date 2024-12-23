package SparkSQL;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */

/**
 * 自定义UDAF函数，实现年龄的平均值
 *     1、实现自定义的 公共类
 *     2、继承 org.apache.spark.sql.expressions.Aggregator;
 *     3、设定泛型
 *         IN : 输入数据类型
 *         BUFF ： 缓冲区数据类型
 *         OUT ： 输出数据类型
 *     4、重写方法 (4(计算机相关) + 2(状态相关))
 */
public class MyAvgAgeUDAF extends Aggregator<Long,AvgAgeBuffer,Long> {
    @Override
    //初始化 缓冲区
    public AvgAgeBuffer zero() {
        return new AvgAgeBuffer(0L,0L);
    }

    @Override
    // 聚合 将输入的值和缓冲区的值做聚合操作
    public AvgAgeBuffer reduce(AvgAgeBuffer buffer, Long in) {
        buffer.setCount(buffer.getCount() + 1);
        buffer.setTotal(buffer.getTotal() + in);
        return buffer;
    }

    @Override
    //合并 缓冲区的值
    public AvgAgeBuffer merge(AvgAgeBuffer b1, AvgAgeBuffer b2) {
        b1.setCount(b1.getCount() + b2.getCount());
        b1.setTotal(b1.getTotal() + b2.getTotal());
        return b1;
    }

    @Override
    // 结束 完成  计算并返回最后结果
    public Long finish(AvgAgeBuffer reduction) {
        return reduction.getTotal() / reduction.getCount();
    }

    @Override
    public Encoder<AvgAgeBuffer> bufferEncoder() {
        return Encoders.bean(AvgAgeBuffer.class);
    }

    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}


