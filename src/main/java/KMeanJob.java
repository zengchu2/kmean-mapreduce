import Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class KMeanJob {

    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("Usage: kmean <in> <k>");
            System.exit(2);
        }
        String inputPath = otherArgs[0];
        String k = otherArgs[1];
        StringBuilder centerPath = new StringBuilder("/keman/tmp/tmpCenters-" + k + "-clusters");
        
        int rounds = 1;
        // Use 2 for testing
        int maxNumberRound = 2;

        while(rounds == 1 || (Utils.compareCenters(
                centerPath.append(rounds - 1).toString(),
                centerPath.append(rounds).toString())) && rounds < maxNumberRound){
            System.out.println("### Round number : " + rounds);
            run(inputPath, 
                    centerPath.append(rounds - 1).toString(),
                    centerPath.append(rounds).toString(),
                    Integer.parseInt(k));
            rounds ++;
        }
    }

    // Run map-reduce job using inputPath and prevCenterPath
    // After job, export new centers to newCenterPath
    private static void run(String inputPath, String prevCenterPath, String newCenterPath, int k) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        config.set("centers", prevCenterPath);
        config.set("data", inputPath);
        config.set("k", k + "");
        Job job = new Job(config, "my-kmean");
        job.setJarByClass(KMeanJob.class);
        job.setMapperClass(KMeanMapper.class);

        job.setReducerClass(KMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));
        System.out.println(job.waitForCompletion(true));
    }
}
