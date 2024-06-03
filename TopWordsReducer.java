import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private PriorityQueue<WordCountPair> queue = new PriorityQueue<>(10, new Comparator<WordCountPair>() {
        public int compare(WordCountPair a, WordCountPair b) {
            return Integer.compare(a.count, b.count);
        }
    });

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
        PriorityQueue<WordCountPair> sortedQueue = new PriorityQueue<>(queue.size(), new Comparator<WordCountPair>() {
            public int compare(WordCountPair a, WordCountPair b) {
                return b.word.compareTo(a.word); // Alphabetical order
            }
        });
        sortedQueue.addAll(queue);
        while (!sortedQueue.isEmpty()) {
            WordCountPair pair = sortedQueue.poll();
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
