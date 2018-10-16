import java.io.IOException;
import java.util.*;

import javax.naming.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CorrectV2 {
    
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList <String> numberArray = new ArrayList();
        String tmpKey = new String();
        String tmpStr = new String();
        String line = value.toString();
        String[] items = line.split(",",-1);
        String date = items[0];
        String place = items[1];
        String type = items[2];

        tmpKey = date + "," + place + "," + type;
        tmpStr += tmpKey;

        for(int idx=3; idx<items.length;idx++){
            String tmpArrayStr = items[idx];
            if(tmpArrayStr.matches(".*[^A-Z0-9-.].*") || tmpArrayStr.equals("")){
                tmpArrayStr="";
                tmpArrayStr = correction(tmpArrayStr, type, items, idx);
                items[idx] = tmpArrayStr;
                tmpStr = tmpStr + "," + tmpArrayStr;              
            }
            else{
                tmpStr = tmpStr + "," + tmpArrayStr;
            }
        }

        context.write(new Text(tmpKey), new Text(tmpStr));
    }

    public String correction(String tmpArrayStr,String type, String[] items, int idx){
        
        if(!type.equals("PH_RAIN") && !type.equals("RAIN_COND") && !type.equals("RAINFALL")){
            if(idx!=3 && idx!= items.length-1){
                String preStr = items[idx-1];
                String nextStr = items[idx+1];
                if(nextStr.matches(".*[^A-Z0-9-.].*") || nextStr.equals("")){
                    return preStr;
                }
                else{
                    float pre = Float.parseFloat(preStr);
                    float next = Float.parseFloat(nextStr);
                    float tokenF = (pre+next)/2;
                    String tokenNew = Float.toString(tokenF);
                    return tokenNew;
                }
            }
            if(idx == 3){
                String nextStr = items[idx+1];
                if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("")){
                    return nextStr;
                }
                else{
                    switch (type) {
                        case "AMB_TEMP":
                            return "20";
                    
                        case "CH4":
                        case "NO":
                        case "SO2":
                            return "1.5";
                        
                        case "CO":
                            return "1.0";
                        
                        case "NMHC":
                            return "0.55";

                        case "NO2":
                            return "0.55";

                        case "NOx":
                            return "17.0";

                        case "O3":
                            return "35";

                        case "PM10":
                            return "38";
                        
                        case "PM2.5":
                            return "35";

                        case "RH":
                            return "65";

                        case "WIND_SPEED":
                        case "THC": 
                            return "2.5";  
                        
                        case "WS_HR":    
                        case "UVB":
                            return "3.5";

                        case "WD_HR":
                            return "100";

                        case "WIND_DIREC":
                            return "200";
                        
                        default:
                            return "0";
                    }
                }
            }
            else{
                String preStr = items[idx-1];
                return preStr;
            }
        }
        else{
            if(idx == 3){
                String nextStr = items[idx+1];
                if(!nextStr.matches(".*[^A-Z0-9-.].*") || !nextStr.equals("") ){
                    return nextStr;
                }
                else{
                    return "NR";
                } 
            }
            else{
                String preStr = items[idx-1];
                return preStr;
            }
        }        
    }
 }

 public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
        for(Text val : values){
            context.write(new Text(), new Text(val));
        }
    }
}

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "Numbers");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(CorrectV2.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
 }
}