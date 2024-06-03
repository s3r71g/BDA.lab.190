import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private PriorityQueue<WordCountPair> queue = new PriorityQueue<>(10, (a, b) -> b.count - a.count);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        queue.add(new WordCountPair(key.toString(), sum));
        if (queue.size() > 10) {
            queue.poll();
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!queue.isEmpty()) {
            WordCountPair pair = queue.poll();
            context.write(new Text(pair.word), new IntWritable(pair.count));
        }
    }

    private static class WordCountPair {
        String word;
        int count;

        WordCountPair(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }
}
