import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;



public class PageRank {
    // Static Variables
    static long count;

    public static enum Total_Nodes {
        // Global Counters
        COUNT
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // Only Mapper Job to get the adjacency list of all the nodes from Wikipedia
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        //job.setNumReduceTasks(0);
        job.setMapperClass(PageRankParsing.PageRankParser.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0] + "/"));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "output/"));
        job.waitForCompletion(true);


        // Getting the total number of nodes in the graph.
        count = job.getCounters().findCounter(Total_Nodes.COUNT).getValue();

        // Driver code to get the mapping from Node names to alias and then pagerank
        conf.setLong("Count", count);
        job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankColumn.pagerankalias.class);
        job.setReducerClass(PageRankColumn.pagerankaliasreducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1] + "output/" + "part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"temp0"+0));
        job.waitForCompletion(true);

        // Driver code to get the list of dangling nodes
        try {
            conf.setLong("Count", count);
            job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankDangling.pagerankdanglingnodesmapper.class);
            job.addCacheFile(new java.net.URI(args[1]+"temp00/part-r-00000"));
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(PageRankDangling.pagerankdanglingnodesreducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1] + "output/" + "part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"dangling"));
            job.waitForCompletion(true);
        }
        catch (Exception e){
            e.printStackTrace();
        }

        // Driver code to get the matrix form of the graph
        try {
            conf.setLong("Count", count);
            job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankSparseMatrix.pagerankmatrixmapper.class);
            job.addCacheFile(new java.net.URI(args[1]+"temp00/part-r-00000"));
            job.setReducerClass(PageRankSparseMatrix.pagerankmatrixreducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1] + "output/" + "part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"matrix"));
            job.waitForCompletion(true);
        } catch (Exception e){
            e.printStackTrace();
        }
        // Driver code to iterate 10 times and then pass it to top K
        for(int i=0;i<10;i++) {
            try {
                conf.setLong("Count", count);
                job = Job.getInstance(conf, "PageRank");
                job.setJarByClass(PageRank.class);
                job.setMapperClass(PageRankIter.pagerankmapper.class);
                job.addCacheFile(new java.net.URI(args[1]+"temp"+i+"0/part-r-00000"));
                job.addCacheFile(new java.net.URI(args[1]+"dangling/part-r-00000"));
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[1]+"matrix/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path(args[1]+"temp"+Integer.toString(i+1)+0));
                job.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // Driver code to get the top K sorted.
        job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankTopK.PageRankSort.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(PageRankTopK.PageRankSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]+"temp100/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"Sorted"));
        job.waitForCompletion(true);

// Driver code to iterate it 10 times for version b
        for(int k=0;k < 10; k++) {
            try {
                job = Job.getInstance(conf, "PageRank");
                job.setJarByClass(PageRank.class);
                job.setMapperClass(PageRankColumnWise.pagerankcolumnwisemapper.class);
                job.addCacheFile(new java.net.URI(args[1] + "temp0" + 0 + "/part-r-00000"));
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(PageRankColumnWise.pagerankcolumnwisereducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[1] + "matrix/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path(args[1] + "Columnwise" + k));
                job.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                conf.setLong("Count", count);
                job = Job.getInstance(conf, "PageRank");
                job.setJarByClass(PageRank.class);
                job.setMapperClass(PageRankColumnWise.pagerankcolumnwisemapper2.class);
                job.addCacheFile(new java.net.URI(args[1] + "temp0" + k + "/part-r-00000"));
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(PageRankColumnWise.pagerankcolumnwisereducer2.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[1] + "Columnwise" + k + "/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path(args[1] + "temp0" + Integer.toString(k + 1)));
                job.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Driver code to find top k for version b.
        job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankTopK.PageRankSort.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(PageRankTopK.PageRankSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]+"temp010/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"Sorted2"));
        job.waitForCompletion(true);

    }
}
