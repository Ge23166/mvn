package count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer extends Reducer<LongWritable, NullWritable, Text, NullWritable> {
    private int sequence = 1;

    @Override
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        for (NullWritable ignored : values) {
            String formatted = String.format("%d %d", sequence, key.get());
            context.write(new Text(formatted), NullWritable.get());
            sequence++;
        }
    }
}