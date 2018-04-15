import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by colntrev on 4/6/18.
 */
public class MinMaxMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final double lower = -5.12;
    private final double upper = 5.12;
    private final int A = 10;
    private int N = 10;
    private int size;
    private SortedMap<Double, String> top = new TreeMap<>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] point = value.toString().trim().split(" ");
        double x = Double.parseDouble(point[0]);
        double y = Double.parseDouble(point[1]);

        // redefining for rastrigin formula
        x = x / size * (upper - lower) + lower;
        y = y / size * (upper - lower) + lower;


        //rastrigin formula
        double rast = 2 * A + (x*x - A * Math.cos(2 * Math.PI * x)) + (y*y - A * Math.cos(2 * Math.PI * y));
        top.put(rast, value.toString());
        if(top.size() > N){
            top.remove(top.lastKey());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        N = context.getConfiguration().getInt("N", 10);
        size = context.getConfiguration().getInt("size", 20);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Double, String> entry: top.entrySet()){
            StringBuilder sb = new StringBuilder();
            String[] values = entry.getValue().split(" ");
            sb.append(entry.getKey());
            sb.append(",");
            sb.append(values[0]);
            sb.append(",");
            sb.append(values[1]);
            context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }
}
