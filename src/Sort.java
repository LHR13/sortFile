import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class Sort {

    public Sort() {
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.out.println("Usage:sortFile<in>[<in>...]<out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "sortFile");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort.sortMap.class);
        job.setPartitionerClass(Partition.class);
        job.setReducerClass(Sort.sortReduce.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }

    public static class sortMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        public sortMap() {
        }

        public void sortMap(Object key, Text value, Context context) throws IOException, InterruptedException {
            Integer num = Integer.parseInt(value.toString());
            context.write(new IntWritable(num), new IntWritable(1));
        }
    }

    public static class sortReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private static IntWritable rank = new IntWritable(1);

        public sortReduce() {
        }

        public void sortReduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(rank, key);
                rank = new IntWritable(rank.get() + 1);
            }
        }
    }

    public static class Partition extends Partitioner<IntWritable, IntWritable> {
        public int getPartition(IntWritable key, IntWritable value, int num_Partition){
            int Maxnumber = 65223;//int型的最大数值
            int bound = Maxnumber / num_Partition + 1;
            int Keynumber = key.get();
            for(int i = 0; i < num_Partition; i++){
                if(Keynumber < bound * i && Keynumber >= bound * (i - 1)) {
                    return i - 1;
                }
            }
            return -1 ;
        }
    }
}
