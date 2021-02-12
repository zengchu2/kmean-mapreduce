import Util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;


public class KMeanMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    
    private ArrayList<ArrayList<Double>> centers;
    private int k;
    
    @Override
    protected void setup(Context context) throws IOException {
        this.centers = Utils.getCenters(
                context.getConfiguration().get("centers")
                ,context.getConfiguration().get("data"));
        this.k = centers.size();
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList<Double> curPoint = Utils.textToPoint(value);
        
        int indexOfClosetCenter = -1;
        double minDistance = Double.MAX_VALUE;
        
        int i = 0;
        while(i < k){
            double distanceToCenter = Utils.distance(centers.get(i), curPoint);
            if(distanceToCenter < minDistance){
                minDistance = distanceToCenter;
                indexOfClosetCenter = i;
            }
            i ++;
            
        }
        
        context.write(new IntWritable(indexOfClosetCenter + 1), value);
    }
}
