package com.neu.temperature;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;


public class PageRank {
    // Static Variables
    static long count;
    static double delta;
    static double delta_prev;

    public static enum Total_Nodes {
        // Global Counters
        COUNT,
        Delta,
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // Only Mapper Job to get the adjacency list of all the nodes from Wikipedia
        Job job = Job.getInstance(conf, "com.neu.pagerank.PageRank");
        job.setJarByClass(PageRank.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(PageRankParser.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]+"/"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"output/"));
        job.waitForCompletion(true);

        // Getting the total number of nodes in the graph.
        count = job.getCounters().findCounter(Total_Nodes.COUNT).getValue();
        delta = job.getCounters().findCounter(Total_Nodes.Delta).getValue();

        // First Iteration of page rank. Count and Delta values are setup in config variables
        conf.setLong("Count",count);
        conf.setDouble("Delta",delta);
        job = Job.getInstance(conf, "com.neu.pagerank.PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankIter1.class);
        job.setReducerClass(PageRankReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]+"output/part-m-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"output1/"));
        job.waitForCompletion(true);


        // Iteratively running 9 map reduce jobs and checks if the pagerank values are converging.
        // Passing the delta and count values using config variables.
        for (int k = 2; k < 10; k++) {
            delta_prev = job.getCounters().findCounter(Total_Nodes.Delta).getValue() - delta;
            conf.setDouble("delta_prev",delta_prev);
            conf.setDouble("Delta",delta);
            job = Job.getInstance(conf, "com.neu.pagerank.PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankIter10.class);
            job.setReducerClass(PageRankReducer1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1] +"output"+Integer.toString(k - 1) + "/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path(args[1] +"output"+Integer.toString(k)));
            job.waitForCompletion(true);
            delta = job.getCounters().findCounter(Total_Nodes.Delta).getValue();
        }

        // Map Reduce jobs to find the Top K values
        job = Job.getInstance(conf, "com.neu.pagerank.PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankSort.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(PageRankSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]+"output9/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"Sorted"));
        job.waitForCompletion(true);
    }

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
                context.getCounter(Total_Nodes.COUNT).increment(1);
            }

        }
    }

    public static class PageRankIter1 extends Mapper<Object, Text, Text, Text> {
        // getting the values of global count using config variable.
        long cnt;
        Configuration conf;
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            cnt = conf.getLong("Count",-1);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // for each node in adjacency list emiting (node, (page-rank + node))
            // emiting the node along with its adjacency list.
            String[] line = value.toString().split(" - ");
            context.write(new Text(line[0]), new Text(line[1]));
            String[] list_nodes = line[1].substring(1, line[1].length() - 2).split(", ");
            int length = list_nodes.length;
            for (String node : list_nodes) {
                if (node.length() != 0) {
                    String pageRank = Double.toString(1.0 / (cnt * length));
                    context.write(new Text(node.replaceAll("\\s", "")), new Text(line[0] + ":" + pageRank));
                }
            }

        }

    }

    public static class PageRankReducer1 extends Reducer<Text, Text, NullWritable, Text> {
        // Setting config variables so that all the reducers get same delta and count values.
        double delt;
        double delt_prev;
        long cnt;
        Configuration conf;
        protected void setup(Context context) throws IOException{
            conf = context.getConfiguration();
            delt = conf.getDouble("Delta",-1);
            delt_prev = conf.getDouble("delta_prev",-1);
            cnt = conf.getLong("Count",-1);
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Initializing the page_rank to be 0.
            double pageRank = 0.0;
            double const_alpha = 0.15;
            String adj_list = "";
            String temp2 = "";
            for (Text value : values) {
                String temp = value.toString();
                // If its adjacency list, then storing its values in a temp variable.
                if (temp.contains("[")) {
                    adj_list = temp;
                    temp2 = adj_list.substring(1, adj_list.length() - 2);
                } else {
                    // accumulating the total page rank values from incoming links.
                    String incomingPage = temp.split(":")[1];
                    pageRank = pageRank + Double.parseDouble(incomingPage);
                }
            }
            // Calculating the total pagerank from incoming links and by random surfing.
            pageRank = ((const_alpha / count) + ((1 - const_alpha) * ((delt_prev / (100000 * count)) + pageRank)));
            // There are few dangling nodes, so adding its value to global counter so that pagerank remains constant.
            if (temp2.length() == 0) {
                long deltavalue = (long) (pageRank * 100000);
                context.getCounter(Total_Nodes.Delta).increment(deltavalue);

            }
            //writing pagerank and adjacency list to file.
            context.write(NullWritable.get(), new Text(key + " : " + Double.toString(pageRank) + "{,}" + adj_list));
        }


    }

    public static class PageRankIter10 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Getting the pagerank, adjacencylist and node from previous iteration file.
            // Passing the adjacency list and outgoing pagerank values to reducer.
            String[] line = value.toString().split(" : ");
            String key_node = line[0];
            String[] pageRank_list = line[1].split("\\{,}");
            double page_rank = Double.parseDouble(pageRank_list[0]);
            // Few Elements do not have a adjacency list but are present in outgoing links. So adding their
            // empty adjacency list and counting them as dangling node.
            if (pageRank_list.length == 1) {
                context.write(new Text(key_node), new Text("[] "));
            } else {
                String[] list_nodes = pageRank_list[1].substring(1, pageRank_list[1].length() - 2).split(",");
                context.write(new Text(key_node), new Text(pageRank_list[1]));
                int length = list_nodes.length;
                for (String node : list_nodes) {
                    if (node.length() != 0) {
                        // emiting pagerank for the outgoing links with the value of pagerank/ total number of elements in adjacency list.
                        String pageRank = Double.toString(page_rank / length);
                        context.write(new Text(node.replaceAll("\\s", "")), new Text(key_node + ":" + pageRank));
                    }

                }

            }
        }

    }

    public static class PageRankSort extends Mapper<Object, Text, Text, Text> {
        // TreeMap to store top 100 node and pageranks
        private TreeMap<Double, String> tmap = new TreeMap<Double, String>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Emiting top 100 values from every mapper buy storing every element in treemap.
            // Removing the first element if there are more than 100 elements in treemap.
            String[] line = value.toString().split(" : ");
            String key_node = line[0];
            String[] pageRank_list = line[1].split("\\{,}");
            double page_rank = Double.parseDouble(pageRank_list[0]);
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
