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

public class Reducer2 extends Reducer<Text, Text, Text, Text> {

	//ArrayList<Text> keyChecker= new ArrayList<>();

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Text output = new Text();
		double total=0;
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






//package Test;
//import java.io.IOException;
//import java.text.DateFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.Date;
//import java.util.HashSet;
//import java.util.List;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//
//public class Reducer2 extends Reducer<Text, Text, Text, Text> {
//	int count=0;
//	
//    
//	//ArrayList<Text> keyChecker= new ArrayList<>();
//
//	
//	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
//		Text output = new Text();
//		ArrayList<String> countryVideos= new ArrayList<>();
//		ArrayList<Datematch> matches= new ArrayList<>();
//		DateFormat format = new SimpleDateFormat("yy.dd.mm");
//
//		Configuration conf = context.getConfiguration();
//		String countryname = conf.get("country1").toUpperCase();
//		//HashSet<String> hs = new HashSet<>();
//		//String country1="US";
//		//String country2="DE";
//        
//		//System.out.println("Keys :" + key);
//    
//		for(Text value: values) {
//			
//			String []v= value.toString().split("\\|");
//			//int index = v.indexOf("|");
//			String views = v[0];
//			try {
//				 
//				java.util.Date d = format.parse(v[1].trim());
//				matches.add(new Datematch(views,d));
//			} catch (ParseException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			 
//			//countryVideos.add(value.toString());
//	      
//
//		}
//		
//           matches.sort(Comparator.comparing(o -> o.date));
//		
//		
//		//for(int i=0;i<matches.size();i++) {
//		//	System.out.println("value for "+key+" : " + a.date+"|"+a.views); //stdout in job history
//		if(matches.size()>2) {
//			double views1= Double.parseDouble(matches.get(0).views.trim());
//			double views2= Double.parseDouble(matches.get(1).views.trim());
//
//			double ViewsPercentage=0;
//			if(views2>views1*2) {
//				
//			ViewsPercentage= ((views2-views1)/views1)*100; // percentage increase
//	        String result=countryname+" : "+ key +" " +String.valueOf(ViewsPercentage).substring(0,7)+"  %";
//            //System.out.println(result); 
//             count++;
//         	 output.set(result);				
//			key.set("");
//			context.write(key,output);
//	
//			}
//			
//	}
//		System.out.println(count);
//
//	}	//}
//		
//		
//		//Collections.reverse(countryVideos);
//		//hs.addAll(countryVideos);
//		//countryVideos.clear();
//		//countryVideos.addAll(hs);
//		//values.forEach(countryVideos::add); // to list conversion
//		
////		for(int i=0;i<countryVideos.size();i++) {
////			//System.out.println("value for "+key+": " + countryVideos.get(i));
////		}
////		//int duplicates= count-countryVideos.size();
////		//System.out.println("Removed duplicates "+ duplicates);
////		
////		
////		List<String> Checked = new ArrayList();
////		Boolean done=false;
////                 
////		if(!countryVideos.isEmpty()) {
////			
////			for(int i = 0; i < countryVideos.size(); i++) {  
////		 		
////				String value[]= countryVideos.get(i).split("\\|");
////				String VideoID= value[0];
////				String TrendingDate=value[1];
////				long Views=Long.parseLong(value[2]);
////				System.out.println("1st loop Values are : "+VideoID+"|"+TrendingDate+"|" +value[2]);
////
////				for(String a:Checked)
////				{
////				  if(a.equals(VideoID)) {
////					done=true; 
////					break;
////				  }
////					
////				}
////				
////				if(done==true) {
////					done=false;
////					continue;
////				}
////												
////				for (int j = i+1; j < countryVideos.size(); j++) {
////         		  
////					String value2[]= countryVideos.get(j).split("\\|");
////					String VideoID2= value2[0];
////					String TrendingDate2=value2[1];
////					long Views2=Long.parseLong(value2[2]);
////					
////					System.out.println("2nd loop Value for"+ VideoID+ ": "+VideoID2+"|"+TrendingDate2+"|" +value2[2]);
////					
////					 
////					if(VideoID.equals(VideoID2) && !TrendingDate.equals(TrendingDate2)) {
////					
////						//System.out.println("++++++++++++++INSIDE LOOP++++++++++++++++");
//////						
////						long ViewsPercentage=0;
////						if(Views2>Views*2) {
//////							
////						ViewsPercentage= ((Views2-Views)/Views)*100; // percentage increase
////				        String result="US"+" : "+ VideoID2 +" " +String.valueOf(ViewsPercentage);
////			            System.out.println(result);
////			            Checked.add(VideoID);
////						break;
//////			         	 output.set(result);				
//////						
//////						//context.write(key,output);
//////					 
////						}
////
////     					}
////                        
////				}
//// 				  
//	 
//	
//
//		
// //                     }
////		output.set("");
////		context.write(key,output);
////
////		
//	
////	output.set("");
//  //  context.write(key,output);
// //  context.write(key,output);
//	//}
//}
//
//
//
//
//
