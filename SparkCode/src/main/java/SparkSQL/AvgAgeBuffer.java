package SparkSQL;

import java.io.Serializable;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */
public class AvgAgeBuffer implements Serializable {
    private long total;
    private long count;

    public AvgAgeBuffer() {
    }

    public AvgAgeBuffer(long total, long count) {
        this.total = total;
        this.count = count;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}