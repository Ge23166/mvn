package count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NumberMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcher = NUMBER_PATTERN.matcher(line);
        while (matcher.find()) {
            String numStr = matcher.group();
            try {
                long num = Long.parseLong(numStr);
                context.write(new LongWritable(num), NullWritable.get());
            } catch (NumberFormatException e) {
                System.err.println("Invalid number: " + numStr);
            }
        }
    }
}