import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text yearMonth = new Text();
    private IntWritable temperature = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.length() > 92) { // Ensure line length is sufficient to parse required fields
            String year = line.substring(15, 19);
            String month = line.substring(19, 21);
            int airTemperature = Integer.parseInt(line.substring(87, 92).trim());
            yearMonth.set(year + "-" + month);
            temperature.set(airTemperature);
            context.write(yearMonth, temperature);
        }
    }
}
