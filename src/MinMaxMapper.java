import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by colntrev on 4/6/18.
 */
public class MinMaxMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
    private int N = 10;
    private SortedMap<Integer, String> top = new TreeMap<>();
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        int frequency = value.get();
        String combined = keyString + ',' + frequency;
        top.put(frequency, combined);
        if(top.size() > N){
            top.remove(top.firstKey());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String value : top.values()){
            context.write(NullWritable.get(), new Text(value));
        }
    }
}
