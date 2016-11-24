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
public class PageRankSparseMatrix {

    public static class pagerankmatrixmapper extends Mapper<Object, Text, LongWritable, Text> {
        long cnt;
        Configuration conf;
        HashMap<String, Long> tmap;

        // Setup method to read the mapped file from cache and then storing it in a hashmap.
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
                tmap.put(list[0], Long.parseLong(value[0]));
            }

        }

        // Reading the adjacency list and then emiting the alias number of the node name with initial pagerank of (1/N)
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" - ");
            String[] list_nodes = line[1].substring(1,line[1].length() -2).split(", ");
            int length = list_nodes.length;
            for (String node: list_nodes){
                if (node.length() != 0) {
                    //tmap.get(nodename) gives you the alias number.
                    context.write(new LongWritable(tmap.get(node.replaceAll("\\s", ""))), new Text(tmap.get(line[0].replaceAll("\\s","")) + ":" + 1.0/length));
                }
            }
        }
    }

    // Grouping all the incoming links with their initial pagerank for each node. All the nodenames are in Aliases
    // This is only a sparse matrix and has incoming links with valid contribution.
    public static class pagerankmatrixreducer extends Reducer<LongWritable, Text, Text, NullWritable> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String temp = "";
            for (Text each : values){
                temp = temp + each+",";
            }
            temp = temp.substring(0, temp.length()-1);
            context.write(new Text(key.toString()+"<><>"+temp),NullWritable.get());
        }
    }
}
