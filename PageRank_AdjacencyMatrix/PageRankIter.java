import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sandeep on 11/19/16.
 */
public class PageRankIter {

    // PageRank Calculator for each iteration for Version A
    public static class pagerankmapper extends Mapper<Object, Text,Text,NullWritable> {
        long cnt;
        Configuration conf;
        HashMap<Long, String> tmap;
        Double danglingcount = 0.0;
        Double danglingnode;

        protected void setup(Context context) throws IOException, InterruptedException{
            //  Getting the total number of nodes present
            conf = context.getConfiguration();
            cnt = conf.getLong("Count",-1);
            // Reading the mapped column-alias file through cache file.
            java.net.URI[] netURI = context.getCacheFiles();
            Path mapped = new Path(netURI[0]);
            BufferedReader reader = new BufferedReader(new FileReader(mapped.toString()));
            String read;
            tmap = new HashMap<Long,String>();

            // Storing the mapped file in hashmap of the form Alias Name -> Node name + Pagerank
            while((read = reader.readLine()) != null){
                String[] list = read.split("::");
                String[] value = list[1].split("\\+++");
                tmap.put(Long.parseLong(value[0]),list[0]+":"+Double.parseDouble(value[1]));
            }

            // Similarly as mapped file, we read dangling file using cache.
            Path dangling = new Path(netURI[1]);
            BufferedReader dang = new BufferedReader(new FileReader(dangling.toString()));
            String temp;

            // Adding the sum of pageranks of all dangling nodes and dividing it with total number of nodes.
            while((temp=dang.readLine())!= null){
                danglingnode = Double.parseDouble(tmap.get(Long.parseLong(temp)).split(":")[1]);
                danglingcount += danglingnode * 1.0 / cnt;

            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parsing the matrix file and then calculating the pagerank using the native formula
            Double pr= 0.0;
            Double pagerank = 0.0;
            String[] line = value.toString().split("<><>");
            String[] values = line[1].split(",");
            Long root = Long.parseLong(line[0]);
            for(String each: values){
                String[] list = each.split(":");
                Long node = Long.parseLong(list[0]);
                pr = pr+(Double.parseDouble(tmap.get(node).split(":")[1])*Double.parseDouble(list[1]));
            }
            pagerank = (0.15/cnt)+(0.85*(pr));
            // Adding back to the hashmap with update pagerank
            String nodename = tmap.get(root).split(":")[0];
            tmap.put(root,nodename+":"+pagerank);
        }

        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // Cleanup function to emit in the format that can be used by the mapper in the next iterations.
            for (Map.Entry<Long, String> entry : tmap.entrySet()) {
                Long key = entry.getKey();
                String nodename = entry.getValue().split(":")[0];
                String pagerank = entry.getValue().split(":")[1];
                context.write(new Text(nodename+"::"+key+"+++"+pagerank),NullWritable.get());
            }
        }
    }
}
