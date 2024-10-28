package task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProductPurchasesMapper extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable hourValue = new IntWritable();
    private IntWritable one = new IntWritable(1);
    private Text productKey = new Text();
    private boolean isHeader = true;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        String line =value.toString();
        
        if(isHeader){
            isHeader = false;
            return ;
        }

        String[] columns = line.split(",");

        if(columns.length == 7){
            String transactionID = columns[0];
            String userID = columns[1];
            String productCategory = columns[2];
            String productID = columns[3];
            String quantitySold = columns[4];
            String revenueGenerated = columns[5];
            String transactionTimestamp = columns[6];

            try {
                // Define the date format
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDateTime transactionDateTime = LocalDateTime.parse(transactionTimestamp, formatter);

                // Extract the hour
                int hour = transactionDateTime.getHour();
                hourValue.set(hour);
                productKey.set(productID);

                // Write the hour as key and transaction details as value
                context.write(productKey, hourValue);
            } catch (Exception e) {
                System.err.println("Error parsing timestamp: " + transactionTimestamp);
            }
           // context.write(productKey, hourValue);
        }
        // context.write(productKey,one);
    }
}
