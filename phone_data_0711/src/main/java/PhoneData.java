import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneData {
    public static class PhoneDataMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\t");
            String phone = values[1];
            long up = Long.parseLong(values[8]);
            long down = Long.parseLong(values[9]);
            FlowBean bean = new FlowBean(up, down);
            context.write(new Text(phone), bean);
        }
    }

    public static class PhoneDataReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> iterable, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            long upSum = 0L;
            long downSum = 0L;
            for (FlowBean bean : iterable) {
                upSum += bean.getUpFlow();
                downSum += bean.getDownFlow();
            }
            context.write(key, new FlowBean(upSum, downSum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PhoneData.class);
        job.setMapperClass(PhoneDataMapper.class);
        job.setReducerClass(PhoneDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/user/student/baoxiao/phone_data_0711/input/HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(job, new Path("/user/student/baoxiao/phone_data_0711/output/"));
        job.waitForCompletion(true);
    }
}
