import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;


public class Bucket implements Serializable {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
private int size ; 
private Vector <String> bucket ; //page path
private Vector <OctPoint> Locations ;
private Vector <Object> ckeys ;
private Object minx ; 
private Object maxx ; 
private Object miny ; 
private Object maxy ; 
private Object minz ; 
private Object maxz ; 
private int count ; 
private Vector<Vector<String>>duplicates;

public Bucket (Object x1,Object y1, Object z1, Object x2,Object y2,Object z2) throws DBAppException { 
	Properties config ; 
	config = new Properties () ; 
	FileInputStream fis;
	try {
		fis = new FileInputStream("src/resources/DBApp.config");
	} catch (FileNotFoundException e1) {
		// TODO Auto-generated catch block
		throw new DBAppException(e1.getMessage());
	}
	try {
		config.load(fis);
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		throw new DBAppException(e1.getMessage());
	} 
	this.size = Integer.parseInt(config.getProperty("MaximumEntriesinOctreeNode"));
	this.bucket = new Vector <String> () ;
	this.Locations = new Vector <OctPoint> () ;
	this.ckeys = new Vector <Object> () ;
	count  = 0 ; 
	minx =  x1 ;
	maxx = x2 ;
	miny= y1; 
	maxy=y2; 
	minz=z1;
	maxz=z2 ; 
	duplicates=new Vector<Vector<String>>();
}

public Vector<Object> getckeys() {
	return ckeys;
}

public void setckeys(Vector<Object> ckeys) {
	this.ckeys = ckeys;
}

public Vector<OctPoint> getLocations() {
	return Locations;
}

public void setLocations(Vector<OctPoint> locations) {
	Locations = locations;
}

public int getSize() {
	return size;
}

public void setSize(int size) {
	this.size = size;
}

public Vector<String> getBucket() {
	return bucket;
}

public void setBucket(Vector<String> bucket) {
	this.bucket = bucket;
}

public Object getMinx() {
	return minx;
}

public void setMinx(Object minx) {
	this.minx = minx;
}

public Object getMaxx() {
	return maxx;
}

public void setMaxx(Object maxx) {
	this.maxx = maxx;
}

public Object getMiny() {
	return miny;
}

public void setMiny(Object miny) {
	this.miny = miny;
}

public Object getMaxy() {
	return maxy;
}

public void setMaxy(Object maxy) {
	this.maxy = maxy;
}

public Object getMinz() {
	return minz;
}

public void setMinz(Object minz) {
	this.minz = minz;
}

public Object getMaxz() {
	return maxz;
}

public void setMaxz(Object maxz) {
	this.maxz = maxz;
}

public int getCount() {
	return count;
}

public void setCount(int count) {
	this.count = count;
}

public Vector<Vector<String>> getDuplicates() {
	return duplicates;
}

public void setDuplicates(Vector<Vector<String>> duplicates) {
	this.duplicates = duplicates;
}
public String displaybounds() {
	OctPoint minPoint = new OctPoint (minx,miny,minz) ; 
	OctPoint maxPoint = new OctPoint (maxx,maxy,maxz) ; 
	
	return "Min point is " + minPoint.display() + " : Max point is " + maxPoint.display();
}



}