import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by sandeep on 11/19/16.
 */
public class PageRankTopK {

    public static class PageRankSort extends Mapper<Object, Text, Text, Text> {
        // TreeMap to store top 100 node and pageranks
        private TreeMap<Double, String> tmap = new TreeMap<Double, String>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Emiting top 100 values from every mapper buy storing every element in treemap.
            // Removing the first element if there are more than 100 elements in treemap.
            String[] line = value.toString().split("::");
            String key_node = line[0];
            double page_rank = Double.parseDouble(line[1].split("\\+++")[1]);
            tmap.put(page_rank, key_node);
            if (tmap.size() > 100) {
                tmap.remove(tmap.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // Emiting a dummy key and (node+page-rank) values.
            for (Map.Entry<Double, String> entry : tmap.entrySet()) {
                context.write(new Text("Dummy"), new Text(entry.getValue() + ":" + Double.toString(entry.getKey())));
            }
        }


    }

    public static class PageRankSortReducer extends Reducer<Text, Text, Text, Text> {
        // TreeMap to store top 100 node and pageranks
        private TreeMap<Double, String> tmap = new TreeMap<Double, String>(Collections.reverseOrder());

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Since all the elements are present in same key, we add the element into tree map and remove the top node
            // if the values in treemap are greater than 100.
            // Emit node and pagerank.
            for (Text value : values) {
                String[] page_node = value.toString().split(":");
                tmap.put(Double.parseDouble(page_node[1]), page_node[0]);
                if (tmap.size() > 100) {
                    tmap.remove(tmap.firstKey());
                }
            }
            for (Map.Entry<Double, String> entry : tmap.entrySet()) {
                context.write(new Text(entry.getValue()), new Text(Double.toString(entry.getKey())));
            }

        }
    }
}
