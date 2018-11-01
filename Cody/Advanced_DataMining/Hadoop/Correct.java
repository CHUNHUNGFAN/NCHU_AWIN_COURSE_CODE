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

public class Correct {
    
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList <String> numberArray = new ArrayList();
        String tmpKey = new String();
        String tmpStr = new String();
        String line = value.toString();
        String[] items = line.split(",",-1);
        /* StringTokenizer tokenizer = new StringTokenizer(line,",");
        String date = tokenizer.nextToken();
        String place = tokenizer.nextToken();
        String type = tokenizer.nextToken(); */
        String date = items[0];
        String place = items[1];
        String type = items[2];

        tmpKey = date + "," + place + "," + type;
        tmpStr += tmpKey;
       
        /* while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            /* boolean i = token.matches(".*[^a-zA-Z0-9-.].*");
            if (i) {
                numberArray.add("");
            }
            else{
                numberArray.add(token);
            } 
            numberArray.add(token);
        } 

        for(int idx=0;idx<numberArray.size();idx++){
            String tmpArrayStr = numberArray.get(idx);
            if(tmpArrayStr.equals("")){
                tmpArrayStr = correction(tmpArrayStr, type,numberArray,idx);
            }
            tmpStr = tmpStr + "," + tmpArrayStr;
        }*/

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
        switch (type) {
            case "AMB_TEMP":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
		    if(!nextStr.equals("")){
                    	float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    } 
                    else{
			return preStr;
		    }
                }
                else{
                    if(idx == 3){
                    String nextStr = items[idx+1];
                    if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("")){
                        return nextStr;
                    }
                    else{
                        return "20";
                    } 
                    }
                    else{
                    String preStr = items[idx-1];
                    return preStr;
                    }
                }

            case "CH4":
            case "NO":
            case "SO2":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else{
			return preStr;
                    }
		}
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "1.5";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "CO":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else {
			return preStr;
                	}
		}
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "1.0";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "NMHC":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    } 
                    else{
			            return preStr;
                	}
		        }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "0.55";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "NO2":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else{
			return preStr;
                	}
		}
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "0.55";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "NOx":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else{
			return preStr;
                	}
		}
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "17.0";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "O3":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "35";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "PM10":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "38";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "PM2.5":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else{
			            return preStr;
                	}
		        }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "35";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }
            
            case "PH_RAIN":
            case "RAIN_COND":
            case "RAINFALL":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    /*if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        //float pre = Float.parseFloat(preStr);
                        /*float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    } */
                    return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
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

            case "RH":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "65";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "THC":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "2.5";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "UVB":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "3.5";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "WD_HR":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "100";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "WIND_DIREC":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "200";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }
            
            case "WIND_SPEED":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "2.5";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }

            case "WS_HR":
                if(idx!=3 && idx!= items.length-1){
                    String preStr = items[idx-1];
                    String nextStr = items[idx+1];
                    if(nextStr.matches(".*[^A-Z0-9-.].*")){
                        nextStr="";
                    }
                    
                    if(!nextStr.equals("")){
                        float pre = Float.parseFloat(preStr);
                        float next = Float.parseFloat(nextStr);
                        float tokenF = (pre+next)/2;
                        String tokenNew = Float.toString(tokenF);
                        return tokenNew;
                    }
                    else return preStr;
                }
                else{
                    if(idx == 3){
                        String nextStr = items[idx+1];
                        if(!nextStr.matches(".*[^A-Z0-9-.].*") && !nextStr.equals("") ){
                            return nextStr;
                        }
                        else{
                            return "3.0";
                        } 
                    }
                    else{
                        String preStr = items[idx-1];
                        return preStr;
                    }
                }
        
            default:
                String tmpArrayStr2 = tmpArrayStr;
                return tmpArrayStr2;
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
    job.setJarByClass(WordCount.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
 }
}
