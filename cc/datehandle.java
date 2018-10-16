import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class datahandle {
        
 public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
	line = line.replaceAll("[-#*]","");
	line = line.replaceAll("x","");
	line = line.replaceAll("NR","0");
        StringTokenizer tokenizer = new StringTokenizer(line);
        String s="";
	double threshold=0.1;
	double time;
	while (tokenizer.hasMoreTokens()) {
		double total=0,maxVal=-1,minVal=-1;
		int count=0;
		String read = tokenizer.nextToken();
		
	      while(line.matches(",,"))
	           line = line.replaceAll(",,",",0,");

		StringTokenizer token_ = new StringTokenizer(read,",");
                StringTokenizer token_2 = new StringTokenizer(read,",");
		time =0;
		while(token_.hasMoreTokens())
		{
			time +=1;
			String word=token_.nextToken();
	//		if(!word.matches(".*[a-zA-z0-9].*") && !word.matches("[\\u4e00-\\u9fa5]+"))
	//			word="0";
			if(word.matches("[0.0-9.0]{0,}"))
			{
				double word_d = Double.parseDouble(word);
				total += word_d;
				count +=1;
				if(maxVal == -1 && minVal == -1)
				{
					minVal = word_d;
					maxVal = word_d;
				}
				maxVal = word_d > maxVal ? word_d : maxVal;
				minVal = word_d < minVal ? word_d : minVal;
			}
		}
		
		while(token_2.hasMoreTokens())
          	{
                        String word=token_2.nextToken();
			String formatVal;
                	        
			if(word.matches("[0.0-9.0]{0,}"))
                        {
                                double word_d = Double.parseDouble(word);
                                if((word_d - minVal)/(maxVal - minVal) <= threshold)
					word=Double.toString(Math.rint((total/count)*10)/10);
			}
			if(s == "")
				s+=word;
			else
	           		s += ","+ word;
                }
		for(;;)
		{	if(time <= 27)
			{
				s += "," + Double.toString(Math.rint((total/count)*10)/10);
				time += 1;
			}else break;
		}
            	context.write(new Text(s),new DoubleWritable(time));
		s="";
	}
    }
 } 
        
 public static class Reduce extends Reducer<Text,DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
      throws IOException, InterruptedException {
       
        for (DoubleWritable val: values) {
        	context.write(key,new Text(""));
	}
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "datahandle");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(datahandle.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
