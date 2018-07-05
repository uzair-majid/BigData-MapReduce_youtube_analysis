package Test;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;



public class SparkTask1 {
	static List<ArrayList<String>> multiplelists = new ArrayList<ArrayList<String>>();

    SparkConf conf = new SparkConf();
    private String categoryName = new String(); // key
	private String videoID_Country = new String(); // value
	//private Text country = new Text(); // key
	 static int count=0;
	static ArrayList<Result> resultList= new ArrayList<>();
	static ArrayList<String> output= new ArrayList<>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inputDataPath = args[0];
		
		//String input[]= args[1].split(",");
		String outputDataPath = args[1];
		//String countryName=input[1];

	    SparkConf conf = new SparkConf();
        conf.setAppName("Spark youtube trending video views per country");
	   
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaRDD<String> Data = sc.textFile(inputDataPath+"Allvideos.csv");
	    
	    JavaPairRDD<String, String> videoIDWithValues = Data.mapToPair(s -> 
    	{  
    	String[] attributes = s.split(",");
    	String k="";
    	String v="";
    	//if(attributes[attributes.length-1].equals(countryName)){
			if((attributes[7].matches("(?!^\\d+$)^.+$") && attributes[7].contains("|"))  && attributes[9].matches("[0-9]+")) { // check if previous index contains not all numbers and contains one '|'  whereas next index contains only numbers
				
				k= attributes[0]+"|"+attributes[attributes.length-1]; //  set videoID and country as key
				//v=attributes[attributes.length-1]+"|"+ attributes[8]+"|" +attributes[1];// set  trending date , total views as value
				v= attributes[8]+"|" +attributes[1];// set  trending date , total views as value

 			//System.out.println("Key:"+context.getCurrentValue());
			}
		


    //	}
    	//System.out.println("Key:"+k + " value: "+v);
    	return new Tuple2<String, String>(k,v);
    	});
	
 	    
	    JavaPairRDD<String, Iterable<String>> groupedData = videoIDWithValues.groupByKey();
    	

//	    for(Tuple2<String, Iterable<String>> line:groupedData.collect()){
//            System.out.println(line._1);
//            System.out.println("=========================================");
//            System.out.println(line._2);
//            
//	    }
	    groupedData.foreach(f->{
		    
    		
	   	 
			ArrayList<Datematch> matches= new ArrayList<>();
			DateFormat format = new SimpleDateFormat("yy.dd.mm");
		 

			for(String value: f._2) {
				//System.out.println(value);
			
				String []v= value.toString().split("\\|");
				if(v.length==2) {
 				String views = v[0];
				try {
					 
					java.util.Date d = format.parse(v[1].trim());
					matches.add(new Datematch(views,d));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 
		      
			}
			
			}
	   //   matches.sort(Comparator.comparing(o -> o.date));
	    //for(int i=0;i<matches.size();i++) {
			//	System.out.println("value for "+f._1+" : " + matches.get(i).date+"|"+matches.get(i).views); //stdout in job history
    	//}
    
			if(matches.size()>=2) {
				double views1= Double.parseDouble(matches.get(0).views.trim());
				double views2= Double.parseDouble(matches.get(1).views.trim());
				double ViewsPercentage=0;

				if(views2>views1*10) {
					
				ViewsPercentage= ((views2-views1)/views1)*100; // percentage increase
		        int index= f._1.indexOf("|")+1;
				//String result=f._1.substring(index)+" : "+ f._1.substring(0,index) +" " +String.valueOf(ViewsPercentage)+"  %";
	           // System.out.println(result);
				resultList.add(new Result(f._1.substring(index), f._1.substring(0,index-1), ViewsPercentage));
				

	          
				}
		}
			
	   });  
//	
	   //for(int i = 0; i < resultList.size(); i++) {   
		//System.out.println( resultList.get(i));
	//}
	    
	//resultList.sort(Comparator.comparing(Result::getAverage).reversed());
	 resultList.sort(Comparator.comparing(Result::getCountry).thenComparing(Result::getAverage).reversed());

	    // Collections.sort(resultList);
	  
//	  for(int i=0;i<resultList.size();) {
//		 // ArrayList<String> temp =new ArrayList<>();
//		 // temp.add(resultList.get(i));
//		 for(int j=i+1;j<resultList.size();j++) {
//		  if(resultList.get(i).substring(0, 2).equals(resultList.get(j).substring(0,2))) {
//			  count++;
//			  
//			 // temp.add(resultList.get(j));
//			 
//			  
//		  }
//		  else {
//			  break;
//		  }
//		 }
//		 ArrayList<String> t = new ArrayList<>();
//		 t.addAll(resultList.subList(i, count+1));
//		 multiplelists.add(t);
//		 i+=count;
//
//	  }
//	  
//	  
//	  
//	  for(int i = 0; i < multiplelists.size(); i++) {   
//		for(int j=0;j<multiplelists.get(i).size();j++) {
//			System.out.println( multiplelists.get(i).get(j));
//
//		}
//	}
	DecimalFormat df = new DecimalFormat("#.#");

	  for(int i = 0; i < resultList.size(); i++) {   
		//	System.out.println( resultList.get(i).country +": "+resultList.get(i).videoID+"   "+ resultList.get(i).average );
			String temp= resultList.get(i).country +"; "+resultList.get(i).videoID+",   "+ df.format(resultList.get(i).average) ;
			output.add(temp);
	  }
	  
//	  for(int i=0;i<resultList.size();i++) {
//		  switch(resultList.get(i).substring(0,2)) {
//		  case "CA":
//			  multiplelists.set(index, element)
// 			  break;
//		  case "DE":
//			  break;
//		  case "FR":
//			  break;
//		  case "GB":
//			  break;
//		  case "US":
//			  break;
	//	  }
	//  }
	   
		//	System.out.println(resultList.size());
	   
	//System.out.println( resultList.size());

	    sc.parallelize(output).saveAsTextFile(outputDataPath);
	   //resultList.saveAsTextFile(outputDataPath + "latest.rating.avg.per.genre");
	    sc.close();
	    
	    
//	    for(Tuple2<String, Iterable<String>> line:groupedData.collect()){
//            System.out.println(line._1);
//            System.out.println("=========================================");
//            System.out.println(line._2);
//            
         
    	
	    }}


//package Test;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.Set;
//
//import org.apache.hadoop.io.Text;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//
//import scala.Tuple2;
//
//public class SparkTask1 {
//	
//	static ArrayList<String> resultList= new ArrayList<>();
//
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		String inputDataPath = args[0];
//		String input[]= args[1].split(",");
//		String outputDataPath = input[0];
// 	    SparkConf conf = new SparkConf();
// 	    
//	    String country1=input[1].toUpperCase();
//		String country2=input[2].toUpperCase();
//       
//	    conf.setAppName("Spark youtube trending video analysis");
//	   
//	    JavaSparkContext sc = new JavaSparkContext(conf);
//	    
//	    JavaRDD<String> Data = sc.textFile(inputDataPath+"ALLvideos.csv");
//	    
//	    JavaPairRDD<String, String> categoryWithValues = Data.mapToPair(s -> 
//    	{  
//    	String[] attributes = s.split(",");
//    	String k="";
//    	String v="";
//    	if(attributes[attributes.length-1].equals(country1) || attributes[attributes.length-1].equals(country2)){
//    			
//			if(attributes[4].matches("[0-9]+") && attributes[6].substring(0, 2).matches("[0-9]+") ) {	
//				
//			           k= attributes[5]; // set categoryName as key
//				       v=attributes[attributes.length-1] +"|"+ attributes[0];// set videoID and country name as value
//				       
//			}
//    	}
//     
//    	  //  System.out.println(k+": "+v);
//		//return null;
//    	
//    	return new Tuple2<String, String>(k,v);
//		
//
//		
//    	}
//    );
//	    
//	    JavaPairRDD<String, Iterable<String>> groupedData = categoryWithValues.groupByKey();
//	    
//	   groupedData.foreach(f->{
//			ArrayList<String> countryVideos= new ArrayList<>();
//			HashSet<String> hs = new HashSet<>();
//			int total=0;
//			int samevideos=0;
//			int count=0;
//			int count2=0;
//	        
//			for(String value: f._2) {
//		     count++;
//		     if(f._1.length()>2)
//				countryVideos.add(value.toString());
//		      
//		      
//			}
//			
//			hs.addAll(countryVideos);
//			countryVideos.clear();
//			countryVideos.addAll(hs);
//			//values.forEach(countryVideos::add); // to list conversion
//			
//			//for(int i = 0; i < countryVideos.size(); i++) {   
//				//System.out.println("value for "+f._1+": " + countryVideos.get(i));
//			//} 
//			//int duplicates= count-countryVideos.size();
//			//System.out.println("Removed duplicates "+ duplicates);
//			
//			
//			if(!countryVideos.isEmpty() ) {
//				count2++;
//				for (int i = 0; i < countryVideos.size(); i++) {
//					if(count2>15) {
//						System.out.println("In loop");
//					}
//					
//					String value= countryVideos.get(i).toString();
//					int index = value.indexOf("|");
//					
//					if(value.substring(0, index).equals(country1)) {
//						total++;
//					}
//					
//					for (int j = i+1; j < countryVideos.size(); j++) {
//					  
//						String value2= countryVideos.get(j);
//						//int index2 = value2.indexOf("|");
//						//value2= value2.substring(index2);
//						if(value2.substring(0,index).equals(country2)) {
//							
//							if(value.substring(index).equals(value2.substring(index))) {
//								samevideos++;
//							}
//
//						}
//					  
//					  }
//				}
//		String result = f._1+": Total in " +country1+": " +String.valueOf(total) + " ;" +country2+" " +samevideos;
//	//	System.out.println( result);
//		resultList.add(result);
//
//		}
//	   });  
//	
//	   for(int i = 0; i < resultList.size(); i++) {   
//		System.out.println( resultList.get(i));
//	}
//	//System.out.println( resultList.size());
//
//	    sc.parallelize(resultList).saveAsTextFile(outputDataPath);
//	   //resultList.saveAsTextFile(outputDataPath + "latest.rating.avg.per.genre");
//	    sc.close();
//	    
//	    
////	    for(Tuple2<String, Iterable<String>> line:groupedData.collect()){
////            System.out.println(line._1);
////            System.out.println("=========================================");
////            System.out.println(line._2);
////            
////        }
//	    
//	     //grouped data
//	    
//	     
//	    
//	    
//	    
//	}
//	
//
//}
