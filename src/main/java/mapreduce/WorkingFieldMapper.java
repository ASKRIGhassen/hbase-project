package mapreduce;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

public class WorkingFieldMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    public static final byte[] CF = "professional".getBytes();
    public static final byte[] FIELD = "field".getBytes();

    private final IntWritable ONE = new IntWritable(1);
    private ImmutableBytesWritable key = new ImmutableBytesWritable();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        key.set(value.getValue(CF, FIELD));
        context.write(key, ONE);

    }
}
