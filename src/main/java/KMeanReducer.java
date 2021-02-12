import Util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class KMeanReducer extends Reducer<IntWritable, Text, Text, Text> {
    
    // Key, [Text] : Center , [Points in this cluster]
    @Override
    protected void reduce(IntWritable key, Iterable<Text> value,Context context)
            throws IOException, InterruptedException {
        ArrayList<ArrayList<Double>> listOfPoints = new ArrayList<ArrayList<Double>>();
        for(Text text : value){
            ArrayList<Double> point = Utils.textToPoint(text);
            listOfPoints.add(point);
        }
        // Recompute the center.
        double[] avg = new double[2];
        
        for(int i = 0; i < listOfPoints.size(); i++){
            avg[0] += listOfPoints.get(i).get(0);
            avg[1] += listOfPoints.get(i).get(1);
        }
        
        avg[0] = avg[0]/listOfPoints.size();
        avg[1] = avg[1]/listOfPoints.size();
        
        context.write(new Text(""), new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
    }
}
