package Test;

public class Datematch{
	
	
	public String views;
	public java.util.Date date;
	public String country;
	
    public Datematch(String views, java.util.Date d) {
		this.date=d;
		this.views=views;
	}
    
    public Datematch(String country,String views, java.util.Date d) {
 		this.country=country;
    	this.date=d;
 		this.views=views;
 	}
}

