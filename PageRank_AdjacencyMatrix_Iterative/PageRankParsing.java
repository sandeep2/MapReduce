import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sandeep on 11/19/16.
 */
public class PageRankParsing {
    public static class PageRankParser extends Mapper<Object, Text, Text, Text> {
        // Parsing line from Wikipedia file to find the adjacency list
        // Incrementing the global counter.
        parser parser;

        protected void setup(Context context) throws IOException, InterruptedException {
            parser = new parser();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = parser.parse(value.toString());
            if (line != null) {
                context.write(new Text(line), new Text(""));
                context.getCounter(PageRank.Total_Nodes.COUNT).increment(1);
            }

        }
    }
}
