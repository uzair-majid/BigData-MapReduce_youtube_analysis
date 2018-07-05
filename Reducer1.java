package Test;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	//ArrayList<Text> keyChecker= new ArrayList<>();

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Text output = new Text();
		double total=5;
		double samevideos=0;
		int count=0;
		double average=0;
		ArrayList<String> countryVideos= new ArrayList<>();
		HashSet<String> hs = new HashSet<>();
		Configuration conf = context.getConfiguration();
 		String country1=conf.get("country1").toUpperCase();
		String country2=conf.get("country2").toUpperCase();
		
		//System.out.println("Keys :" + key);
    
		for(Text value: values) {
	    // count++;
			countryVideos.add(value.toString());
	      
	      
		}
//		for(int i = 0; i < countryVideos.size(); i++) {   
//			System.out.println("value for "+key+": " + countryVideos.get(i));
//		} 
	//	remove duplicates
		hs.addAll(countryVideos);
		countryVideos.clear();
		countryVideos.addAll(hs);
		//values.forEach(countryVideos::add); // to list conversion
		
//		for(int i = 0; i < countryVideos.size(); i++) {   
//			System.out.println("value for "+key+": " + countryVideos.get(i));
//		} 
		//int duplicates= count-countryVideos.size();
		//System.out.println("Removed duplicates "+ duplicates);
		
		
		if(!countryVideos.isEmpty()) {
			
			for (int i = 0; i < countryVideos.size(); i++) {
			  
				String value= countryVideos.get(i).toString();
				int index = value.indexOf("|");
				
				if(value.substring(0, index).equals(country1)) {
					total++;
				
				
				for (int j = 0; j < countryVideos.size(); j++) {
				  
					String value2= countryVideos.get(j);
					//int index2 = value2.indexOf("|");
					//value2= value2.substring(index2);
					if(value2.substring(0,index).equals(country2)) {
						//System.out.println(value2.substring(0,index)+" | "+ country2 );
						
						if(value.substring(index).equals(value2.substring(index))) {
								
							samevideos++;

							System.out.println(value+" | "+  value2 + "  "+ samevideos );
							break;
						}

					}
				  
				  }
				}
			}
	if (total>0) {
		DecimalFormat df = new DecimalFormat("#.#");

		average= (samevideos/total)*100;
		String result = "Total in " +country1+": " +String.valueOf(total) + " ;" +country2+" " +df.format(average)+ " %";
		System.out.println(result);
		output.set(result);	
		context.write(key,output);
	}
	else {
		
		String result = "Total in " +country1+": " +String.valueOf(total) + " ;" +country2+" " + 0;
		System.out.println(result);
		output.set(result);	
		context.write(key,output);
	}

		
//		if(total>0) {
//			int average=(samevideos/total)*100;
//
//			String result = " :" + country1+": " +String.valueOf(total) + " ;" +country2+" " + average;
//			output.set(result);		
//		}
//		else
//		{
//			
//			String result = " :" + country1+": " +String.valueOf(total) + " ;" +country2+" " +samevideos;
//			output.set(result);		
//			
//		}
		
		//output.set(String.valueOf(total));
	
		
	}
	}
}





