package Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class Utils {
    // Parse Text to a ArrayList<Double> representing our data points
    public static ArrayList<Double> textToPoint(Text value) {
        return TextUtils.textToPoint(value);
    }

    // Compare new and old centers, if 
    public static boolean compareCenters(String centerPath, String newCenterPath) throws IOException {
        return NumericUtils.centersHaveUpdated(centerPath, newCenterPath);
    }

    
    // Return the manhattan distance of two coordinates.
    public static Double distance(ArrayList<Double> c1,ArrayList<Double> c2){
        return NumericUtils.distance(c1, c2);
    }


    // Read data point files from given path, and then return list of data points.
    public static ArrayList<ArrayList<Double>> getCenters(String centerPath, String dataInput) throws IOException {
        // if centerPath file exists, then read it, else use random points from datainput file
        return MyIOUtils.IOReadCentersFromPath(centerPath);
    }
}

class NumericUtils {
    public static boolean centersHaveUpdated(String centerPath, String newCenterPath) throws IOException {
        ArrayList<ArrayList<Double>> prevCenters = MyIOUtils.IOReadCentersFromPath(centerPath);
        ArrayList<ArrayList<Double>> newCenters = MyIOUtils.IOReadCentersFromPath(newCenterPath);
        
        boolean needOneMoreRound = false;
        for(int i = 0; i < prevCenters.size(); i++){
            
            if(distance(prevCenters.get(i), newCenters.get(i)) > 0.5){
                needOneMoreRound = true;
                break;
            }
        }
        
        return needOneMoreRound;       
    }

    public static Double distance(ArrayList<Double> c1,ArrayList<Double> c2){
        Double x2 = Math.pow(c1.get(0) + c2.get(0), 2);
        Double y2 = Math.pow(c1.get(1) + c2.get(1), 2);
        return Math.sqrt(x2 + y2);
    }}


class TextUtils {
    public static ArrayList<Double> textToPoint(Text value) {
        ArrayList<Double> res = new ArrayList<Double>();
        String[] fields = value.toString().split(",");
        for(String field: fields){
            res.add(Double.parseDouble(field));
        }
        return res;
    }
    
}

class MyIOUtils {
    // If exists, pick them, else randomly pcik 
    public static ArrayList<ArrayList<Double>> IOReadCentersFromPath(String path) throws IOException {
        ArrayList<ArrayList<Double>> res = new ArrayList<ArrayList<Double>>();
        Path pathToCenters = new Path(path);
        Configuration config = new Configuration();
        FileSystem fs = pathToCenters.getFileSystem(config);

        FSDataInputStream inputStream = fs.open(pathToCenters);
        LineReader lineReader = new LineReader(inputStream, config);
        Text line = new Text();

        while(lineReader.readLine(line) > 0){
            res.add(TextUtils.textToPoint(line));
        }

        lineReader.close();
        return res;
    }    

    public static boolean deleteFileAtPath(String path){
        try{
            FileSystem fs = new Path(path).getFileSystem(new Configuration());
            fs.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

//    public static void initializeCenters(String centerPath, int k) throws IOException {
//        ArrayList<ArrayList<Double>> data = IOReadCentersFromPath(centerPath);
//        Integer[] randomIndex = new Integer[4];
//        int cur = 0;
//        Random rand = new Random();
//        while(cur < k){
//            int curRandom = rand.nextInt(data.size() - 1);
//            boolean canUpdate = true;
//            for(int i = 0; i < cur; i++){
//                if (randomIndex[i] == curRandom) {
//                    canUpdate = false;
//                    break;
//                }
//            }
//            if(canUpdate) cur ++;
//        }
//        
//        
//        
//    }
}