package Test;

public class Result {
	public String country;
	public double average;
	public String videoID;
	
	
	public Result (String c,String v , double a){
		this.country=c;
		this.videoID=v;
		this.average=a;
	
	}


	public String getCountry() {
		return country;
	}


	public void setCountry(String country) {
		this.country = country;
	}


	public double getAverage() {
		return average;
	}


	public void setAverage(Double average) {
		this.average = average;
	}


	public String getVideoID() {
		return videoID;
	}


	public void setVideoID(String videoID) {
		this.videoID = videoID;
	}
	
}
