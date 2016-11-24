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
public class PageRankColumnWise {
    public static class pagerankcolumnwisemapper extends Mapper<Object, Text, LongWritable, Text> {

        // Parsing the matrix file and then sending to the reducer.
        // First mapper would emit of the format (nodename , outlink+pagerank)
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("<><>");
            String rootnode = line[0];
            String[] list_nodes = line[1].split(",");
            for (String node : list_nodes) {
                String[] list = node.split(":");
                Long listdivide = Long.parseLong(list[0]);
                Double contribution = Double.parseDouble(list[1]);
                context.write(new LongWritable(Long.parseLong(rootnode)), new Text(listdivide + "::" + contribution));
            }
        }
    }

    public static class pagerankcolumnwisereducer extends Reducer<LongWritable, Text, Text, NullWritable> {
        long cnt;
        Configuration conf;
        HashMap<Long, String> tmap;
        // Setup method is done to create map of pagerank, alias numbers and node name
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            cnt = conf.getLong("Count", -1);
            java.net.URI[] netURI = context.getCacheFiles();
            Path mapped = new Path(netURI[0]);
            BufferedReader reader = new BufferedReader(new FileReader(mapped.toString()));
            String read;
            tmap = new HashMap<Long, String>();

            while ((read = reader.readLine()) != null) {
                String[] list = read.split("::");
                String[] value = list[1].split("\\+++");
                tmap.put(Long.parseLong(value[0]), list[0] + ":" + Double.parseDouble(value[1]));
            }
        }

        // For each incoming element (key, incoming link contribution * its pagerank) is emitted
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double pr;
            Double contr = 0.0;
            for (Text each : values) {
                //.out.println(each.toString());
                String[] list = each.toString().split("::");
                pr = Double.parseDouble(tmap.get(Long.parseLong(list[0])).split(":")[1]);
                contr = pr * Double.parseDouble(list[1]);
                context.write(new Text(key + "::" + contr), NullWritable.get());
            }
            ;
        }
    }

    public static class pagerankcolumnwisemapper2 extends Mapper<Object, Text, LongWritable, Text> {

        // In this mapper (nodename alias and it's contribution) is sent to reducer.
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("::");
            context.write(new LongWritable(Long.parseLong(line[0])), new Text(line[1]));
        }
    }

    public static class pagerankcolumnwisereducer2 extends Reducer<LongWritable, Text, Text, NullWritable> {
        long cnt;
        Configuration conf;
        HashMap<Long, String> tmap;
        // A setup method is written to create a hashmap of alias numbers, node names and pageranks
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            cnt = conf.getLong("Count", -1);
            java.net.URI[] netURI = context.getCacheFiles();
            Path mapped = new Path(netURI[0]);
            BufferedReader reader = new BufferedReader(new FileReader(mapped.toString()));
            String read;
            tmap = new HashMap<Long, String>();

            while ((read = reader.readLine()) != null) {
                String[] list = read.split("::");
                String[] value = list[1].split("\\+++");
                tmap.put(Long.parseLong(value[0]), list[0] + ":" + Double.parseDouble(value[1]));
            }
        }

        // In reducer we just have to add all the pagerank contributions for each incoming node and then use the native
        // formula to find the pagerank of that node.
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double contr = 0.0;
            Double pagerank = 0.0;
            for (Text each : values) {
                contr += Double.parseDouble(each.toString());
            }
            pagerank = (0.15/cnt)+(0.85*(contr));
            String nodename = tmap.get(Long.parseLong(key.toString())).split(":")[0];
            context.write(new Text(nodename+"::"+key+"+++"+pagerank),NullWritable.get());
        }
    }
}


