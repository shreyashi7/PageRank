

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ReducerStage1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public static String count;

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        int N = Integer.parseInt(count);

        String title = key.toString();
        double rank = 0.0;
        double votes = 0.0;
        double d = 0.85f;
        String outlinks = "";
        boolean redLink=true;
        boolean pageNoOutlinks = false;

        while(values.hasNext()){
            String svalue = values.next().toString();

            if ( svalue.indexOf("\t") != -1){

                String[] svalues = svalue.split("\t");

                
                if (svalues[0].equals("#")){
                    for(int i= 1; i < svalues.length; i++){
                        outlinks += "\t" + svalues[i];
                    }
                    continue;
                }

             
                if (svalues[0].equals("$")){
                    redLink = false;
                    continue;
                }

            }else{

                if (svalue.equals("#")){
                    pageNoOutlinks = true;
                }else{
                    votes += Double.parseDouble(svalue);
                }

            }
        }

        if (!redLink){
            rank = (0.15/N) + d * votes ;

           
            if (pageNoOutlinks){
                output.collect(new Text(title), new Text ( Double.toString(rank)  ) );
            }else{ 
                output.collect(new Text(title), new Text ( Double.toString(rank) + outlinks ) );
            }
        }
    }
}