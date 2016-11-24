import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by sandeep on 11/19/16.
 */
public class PageRankDangling {

    public static class pagerankdanglingnodesmapper extends Mapper<Object, Text,Text,Text> {
        // Reading the adjacency list and then find if the number of outlinks for a node are zero.
        // Then use a dummy key to group all such elements.
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" - ");
            String list_nodes = line[1].substring(1, line[1].length() - 2);
            if (list_nodes.length() == 0){
                context.write(new Text("Dangling"), new Text(line[0]));
            }
        }
    }

    public static class pagerankdanglingnodesreducer extends Reducer<Text, Text, LongWritable, NullWritable> {
        long cnt;
        Configuration conf;
        HashMap<String, Long> tmap;
        // A setup method is to create hashmap a mapping of nodename and its alias number
        protected void setup(Context context) throws IOException, InterruptedException{
            conf = context.getConfiguration();
            cnt = conf.getLong("Count",-1);
            java.net.URI[] netURI = context.getCacheFiles();
            Path mapped = new Path(netURI[0]);
            BufferedReader reader = new BufferedReader(new FileReader(mapped.toString()));
            String read;
            tmap = new HashMap<String,Long>();

            while((read = reader.readLine()) != null){
                String[] list = read.split("::");
                String[] value = list[1].split("\\+++");
                tmap.put(list[0],Long.parseLong(value[0]));
            }

        }
        // All the dangling nodes are emitted to a file which is used during iteration.
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for (Text each : values){
                context.write(new LongWritable(tmap.get(each.toString())), NullWritable.get());
            };
        }
    }
}
