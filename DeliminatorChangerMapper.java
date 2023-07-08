import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeliminatorChangerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context) 
  throws IOException, InterruptedException {
    String line = value.toString();
    String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    for (int i = 0; i < fields.length; i++) {
        if (fields[i].startsWith("\"(") || fields[i].endsWith("\")\"")) {
            continue; // skip comma delimiter after parenthesis
        }
        fields[i] = fields[i].replaceAll(",", "%%%");
    }

    String newLine = String.join("%%%", fields);
    context.write(NullWritable.get(), new Text(newLine));
}
}