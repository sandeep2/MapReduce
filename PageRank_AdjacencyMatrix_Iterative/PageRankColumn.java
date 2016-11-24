import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sandeep on 11/19/16.
 */
public class PageRankColumn {

    public static class pagerankalias extends Mapper<Object, Text, Text, NullWritable> {
        // Reading the adjacency list file and parsing it to find all nodes and its outlinks.
        // Emiting the node and all its links to reducer.
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" - ");
            String[] list_nodes = line[1].substring(1, line[1].length() - 2).split(", ");
            context.write(new Text(line[0]), NullWritable.get());
            for (String node : list_nodes) {
                if (node.length() != 0) {
                    context.write(new Text(node.replaceAll("\\s", "")), NullWritable.get());
                }
            }

        }
    }

    public static class pagerankaliasreducer extends Reducer<Text, Text, Text, NullWritable> {
        // Setting config variables so that all the reducers get same delta and count values.
        // For each incoming node a alias name is given and incremented for every new node found.
        // Alias name and node name with pagerank are emitted.
        long cnt;
        Configuration conf;
        int temp;
        protected void setup(Context context) throws IOException, InterruptedException{
            temp = 1;
            conf = context.getConfiguration();
            cnt = conf.getLong("Count",-1);
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            context.write(new Text(key+"::"+temp+"+++"+1.0/cnt), NullWritable.get());
            temp++;

        }


    }

}
