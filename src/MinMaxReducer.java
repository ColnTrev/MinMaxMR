import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by colntrev on 4/6/18.
 */
public class MinMaxReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
    private int N = 10;
    private SortedMap<Integer, String> top = new TreeMap<>();
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           for(Text value : values){
               String valueString = value.toString().trim();
               String[] tokens = valueString.split(",");
               String keyValue = tokens[0];
               int frequency = Integer.parseInt(tokens[1]);
               top.put(frequency, keyValue);
               if(top.size() > N){
                   top.remove(top.firstKey());
               }
           }

           List<Integer> keys = new ArrayList<>(top.keySet());
           for(int i = keys.size()-1; i >= 0; i--){
               context.write(new IntWritable(keys.get(i)), new Text(top.get(keys.get(i))));
           }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        N = context.getConfiguration().getInt("N", 10);
    }
}
