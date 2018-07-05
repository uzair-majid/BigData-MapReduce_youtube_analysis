package Test;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	int count=0;	

	//private Text categoryName = new Text(); // key
	//private Text videoID_Country = new Text(); // value
	private Text category_name = new Text(); // key
	private Text country_VideoID = new Text(); // value
	int count2=0;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
 		String country1=conf.get("country1").toUpperCase();
		String country2=conf.get("country2").toUpperCase();
		String line= value.toString();
		String [] attributes = line.split(",");
		//count++;
		
		//if(count<3) {			
		//	System.out.println("the attributes are:"+ attributes[0]+" | " +attributes[5] +" |"+ attributes[attributes.length-1]  );
	  
		//}
		//if(attributes[4].matches("[0-9]+")  ) {	
			//	&& attributes[5].matches("/^([^0-9]*)$/")
			
		if(attributes[attributes.length-1].equals(country1) || attributes[attributes.length-1].equals(country2)){
		if( attributes[5].matches("\\D*") && attributes[4].matches("[0-9]+")  ) {	
			//count++;
			category_name.set(attributes[5]); 		// set categoryName as key

			country_VideoID.set(attributes[attributes.length-1] +"|"+ attributes[0]);// set videoID and country name as value
			context.write(category_name, country_VideoID);
			System.out.println(category_name+ "|"+ country_VideoID);
		}
			else if(attributes[5].matches("[0-9]+")) {
				category_name.set(attributes[6]);
				country_VideoID.set(attributes[attributes.length-1] +"|"+ attributes[0]);// set videoID and country name as value
				context.write(category_name, country_VideoID);
				System.out.println(category_name+ "|"+ country_VideoID);
		
			}
			else if(attributes[5].matches("\\D*") && attributes[4].matches("\\D*") ) {
				int t= 5;
				//while(t<attributes.length) {
				
					if(attributes[t+1].matches("[0-9]+") ) {
						category_name.set(attributes[t+2]);
						country_VideoID.set(attributes[attributes.length-1] +"|"+ attributes[0]);// set videoID and country name as value
						context.write(category_name, country_VideoID);
						System.out.println(category_name+ "|"+ country_VideoID);	
						//t++;
						//break;
					}
					if(attributes[t+2].matches("[0-9]+") ) {
						category_name.set(attributes[t+3]);
						country_VideoID.set(attributes[attributes.length-1] +"|"+ attributes[0]);// set videoID and country name as value
						context.write(category_name, country_VideoID);
						System.out.println(category_name+ "|"+ country_VideoID);	
						//t++;
						//break;
					}
				//}
			}
		}
		
				
		//else if(attributes[attributes.length-1].equals(country2)){
			//if( attributes[5].matches("\\D*") ) {	
				//attributes[4].matches("[0-9]+")&&
				//count++;
//			country.set(attributes[5]); // set categoryName as key
//			category_VideoID.set(attributes[attributes.length-1] +"|"+ attributes[0]);// set videoID and country name as value
//			//count++;
//			context.write(country, category_VideoID);
//			System.out.println(country+ "|"+ category_VideoID);

//			categoryName.set(attributes[5]); // set categoryName as key
//			videoID_Country.set(attributes[0] +"|"+ attributes[attributes.length-1]);// set videoID and country name as value
			//count2++;
			//}
		//}
			
			//country.set(attributes[5]+"|"+attributes[3]); // set categoryName as key
		//	category_VideoID.set(attributes[attributes.length-1] +"|"+ attributes[0]);// set videoID and country name as value

			
			//if(count<3) {
				
			//System.out.println("CATEGORY_NAME: "+categoryName+" "+"VIDEOID_COUNTRY_NAME: "+videoID_Country);
			//}	
			//context.write(categoryName, videoID_Country);
			
				
		//}
			
			//System.out.println("CATEGORY_NAME | VIDEOID_COUNTRY : "+context.getCurrentValue());
		
		
	
	}
}
