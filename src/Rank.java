
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import java.io.File;

public class Rank {
    public static long count = 0;
    public static void main(String[] args) throws Exception {
        Rank mainObject = new Rank();
        String input = args[0];
        String output = args[1];
        int noOfIterations = 8;
        String outlink = output ;
        String temp = input;
        String mergeOutlinks="outlinks_merged";

        mainObject.MergeFiles(input, outlink + "/temp/"+ mergeOutlinks);
        
        String pageOutlink="PageRank.n.out";
        String Iter1 = "PageRank.iter1.out";
        String Iter8 = "PageRank.iter8.out";

        String[] iterations = new String[noOfIterations+1];

        for ( int i =0; i <= noOfIterations ; i++) {
            iterations[i] = "PageRank.iter" + Integer.toString(i) +".out";
        }

        mainObject.InlinkCountGenerationJob(outlink + "/temp/"+ mergeOutlinks,outlink+ "/temp/"+pageOutlink);
        mainObject.RunCalculatorStage1(outlink + "/temp/"+ mergeOutlinks, outlink + "/temp/"+ iterations[0], outlink + "/temp/"+ pageOutlink);
        for ( int i =1; i <= noOfIterations ; i++){
            mainObject.RankCalculatorJob(outlink + "/temp/"+ iterations[i-1], outlink + "/temp/"+ iterations[i], outlink + "/temp/"+ pageOutlink);
        }

        mainObject.SortJob(outlink + "/temp/"+iterations[1], outlink + "/temp/"+ Iter1, outlink + "/temp/"+ pageOutlink);
        mainObject.SortJob(outlink + "/temp/"+iterations[8], outlink+ "/temp/"+Iter8, outlink + "/temp/"+ pageOutlink);
       
        System.out.println(outlink + "/temp/" + pageOutlink); 
        System.out.println(outlink + "/temp/" + Iter1); 
        System.out.println(outlink + "/temp/" + Iter8); 
        
      // mainObject.MergeFiles(outlink + "/temp/" + Iter1, outlink + "iter1.out");
      //mainObject.MergeFiles(outlink + "/temp/" + Iter8, outlink + "iter8.out");
      // mainObject.MergeFiles(outlink + "/temp/" + pageOutlink, outlink + "num_nodes");
      

       mainObject.MergeFiles(outlink + "/temp/" + Iter1, outlink + "/iter1.out");
       mainObject.MergeFiles(outlink + "/temp/" + Iter8, outlink + "/iter8.out");
       mainObject.MergeFiles(outlink + "/temp/" + pageOutlink, outlink + "/num_nodes");
      
       
    }

   
    
    public void InlinkCountGenerationJob(String input, String output) throws IOException{
        JobConf conf = new JobConf(Rank.class);
        conf.setJarByClass(Rank.class);

      

        
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(LinkMapper.class);
        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(NullWritable.class);

      
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(LinkReducer.class);

       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        
        JobClient.runJob(conf);
    }

    public void RunCalculatorStage1 (String input, String output, String linkcountfile) throws IOException {
        JobConf conf = new JobConf(Rank.class);
        conf.setJarByClass(Rank.class);

        
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(MapperStage2.class);

      
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(ReducerStage2.class);

       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        Integer count = readCountFromFile(linkcountfile, conf);
        conf.set("count", Integer.toString(count));

        
        JobClient.runJob(conf);
    }


    public void RankCalculatorJob(String input, String output, String linkcountfile) throws IOException{
        JobConf conf = new JobConf(Rank.class);
        conf.setJarByClass(Rank.class);

   
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(MapperStage1.class);

        
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(ReducerStage1.class);

        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        Integer count = readCountFromFile(linkcountfile, conf);
        conf.set("count", Integer.toString(count));

       
        JobClient.runJob(conf);
    }

    public void SortJob(String input, String output, String linkcountfile) throws IOException{
        JobConf conf = new JobConf(Rank.class);
        conf.setJarByClass(Rank.class);

        
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(MapSort.class);

       
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(ReduceSort.class);

        
        conf.setMapOutputKeyClass(DoubleWritable.class);
        conf.setMapOutputValueClass(Text.class);

        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

       
        conf.setOutputKeyComparatorClass(KeyComparator.class);
        conf.setOutputValueGroupingComparator(GroupComparator.class);
        conf.setPartitionerClass(FirstPartitioner.class);

        
        Integer count = readCountFromFile(linkcountfile, conf);
        conf.set("count", Integer.toString(count));

       
        JobClient.runJob(conf);
    }

    public static class FirstPartitioner implements Partitioner<DoubleWritable, Text> {

        @Override
        public void configure(JobConf job) {}

        @Override
        public int getPartition(DoubleWritable key, Text value, int numPartitions) {
            double d = (Double.parseDouble(key.toString()));
            int n =(int) d * 100;
            return (int)(n / numPartitions) ;
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(DoubleWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable ip1 = (DoubleWritable) w1;
            DoubleWritable ip2 = (DoubleWritable) w2;
            return ip1.compareTo(ip2);
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(DoubleWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable d1 = (DoubleWritable) w1;
            DoubleWritable d2 = (DoubleWritable) w2;
            int cmp = d1.compareTo(d2);
            return cmp * -1; //reverse
        }
    }

    private Integer readCountFromFile (String filepath, Configuration conf) throws IOException
    {
        String fileName = filepath + "/part-00";
        Integer count = 1;
        BufferedReader br = null;
        FileSystem fs = null;
        Path path ;//= new Path(fileName);
        NumberFormat nf = new DecimalFormat("000");
        Configuration config = new Configuration();

        try {

            path = new Path (fileName + nf.format(0));
            fs = path.getFileSystem(new Configuration());
            String line = "";

            if (!fs.isFile(path)){
                fileName = filepath + "/part-00";
            }

           
            for (int i=0; i<=999; i++){
                path = new Path (fileName + nf.format(i));
                fs = path.getFileSystem(config);
                if (fs.isFile(path)){
                    br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    line = br.readLine();

                    if (line!= null && !line.isEmpty() && line.length() >= 2)
                        break;
                }
            }

            if (line != null && !line.isEmpty())
            {
                String splits = line ;
                count = Integer.parseInt(splits);
                return count;
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                br.close();
                fs.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return count;
    }

    private void MergeFiles(String input, String output) throws  IOException {
        String fileName = input + "/part-00";
        NumberFormat nf = new DecimalFormat("00");

        Configuration conf = new Configuration();
        FileSystem outFS = null;

        try {

            Path outFile = new Path(output);
            outFS = outFile.getFileSystem(new Configuration());
            if (outFS.exists(outFile)){
                System.out.println(outFile + " already exists");
                System.exit(1);
            }

            FSDataOutputStream out = outFS.create(outFile);

            Path inFile = new Path (fileName + nf.format(0));
            FileSystem inFS = inFile.getFileSystem(new Configuration());

            if (!inFS.exists(inFile)){
                fileName = input + "/part-00";
            }

            //This will generate file names -00001 to -00999, I hope this is sufficient
            for (int i=0; i<=999; i++){

                inFile = new Path (fileName + nf.format(i));
                System.out.println(inFile);
                inFS = inFile.getFileSystem(new Configuration());

                if (inFS.isFile(inFile)){

                    int bytesRead=0;
                    byte[] buffer = new byte[4096];

                    FSDataInputStream in = inFS.open(inFile);

                    while ((bytesRead = in.read(buffer)) > 0) {
                        out.write(buffer, 0, bytesRead);
                    }

                    in.close();
                }else{
                    break;
                }

                inFS.close();
            }

            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                outFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
  }