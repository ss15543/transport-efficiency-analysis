import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubsetMapper extends Mapper<Object, Text, Text, Text> {
  
  @Override
  public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
  
    String line = value.toString();
    String[] field = line.split("%%%");
    
    // making sure the array has at least 29 elements 
    if (field.length >= 29 ) {

      // for uniformity
      String borough = field[2];
      if (borough.equalsIgnoreCase("staten island")) {
        borough = "STATENISLAND";
      }
      if (borough.equalsIgnoreCase("")) {
        borough = "UNKNOWN";
      } 

      //replacing null values for location information by 0000 
      for (int i = 3; i <= 5; i++){
        if (field[i] == null || field[i].isEmpty()) {
          field[i] = "0000";
        }
      }

      // removing space from any column value 
      for (int i = 0; i < field.length; i++){
        if (field[i].contains(" ")) {
          field[i] = field[i].replaceAll(" ", "");
        }
      }

      // calculating the number of vehicles involved in a collision
      int vehicleCount = 0;
      int lastVehicleIndex = field.length - 1;

      if (lastVehicleIndex >= 24) {
        // vehicle type codes are in the last five columns
        for (int i = lastVehicleIndex; i >= 24; i--) {
            if (field[i] != null && field[i].matches("[a-zA-Z/]+")) {
                vehicleCount++;
            }
        }
      }

       /*
      //for remaining null values, we replace with a string 'null'
      for (int i = 0; i < field.length; i++){
        if (field[i] == null || field[i].isEmpty()) {
          field[i] = "null";
        }
      }
      */
      
      //to get subset features
      String[] subset_fields = new String[] {field[23], field[0], field[1], borough, field[3], field[4], field[5],
              field[10], field[11], field[12], field[13], field[14], field[15], field[16], field[17], 
              field[18], String.valueOf(vehicleCount)};

      String subset_data = String.join(",", subset_fields);
      context.write(new Text(subset_data), value);  
    }
  }
}
     
