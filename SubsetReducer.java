import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SubsetReducer extends Reducer<Text, Text, NullWritable, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
    throws IOException, InterruptedException {
      
      int counter = 0;
      //counter increments for every new value for a key
      for (Text value : values) {
        counter++;
      }
      // this will remove any duplicate records
      if (counter <= 1){
        context.write(NullWritable.get(), key);
      }
    }
}
