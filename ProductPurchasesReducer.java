package task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class ProductPurchasesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outputValue =new IntWritable(0);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        int maxHourCount = 0;
        int maxHour = -1;
        HashMap<Integer,Integer> hourCountMap = new  HashMap<>(); 
        for(IntWritable value : values){
            int hour = value.get();
            int hourCount = hourCountMap.getOrDefault(hour,0);
            hourCountMap.put(hour,++hourCount);
            if(hourCount>maxHourCount){
                maxHourCount = hourCount;
                maxHour = hour;
            }
        }
        outputValue.set(maxHour);
        context.write(key,outputValue);
    }
}
