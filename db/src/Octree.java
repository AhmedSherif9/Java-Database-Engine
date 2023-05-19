import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector; 

public class Octree<T> implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Bucket Bucket; 

    private Octree<T>[] children = new Octree[8];  
    private T object;
    private String coloumn1 ;
    private String coloumn2 ;
    private String coloumn3 ;
    
    public Octree(Object x1,Object y1, Object z1, Object x2,Object y2,Object z2, String str1, String str2, String  str3) throws DBAppException{
        Bucket = new Bucket ( x1, y1,  z1,  x2, y2, z2) ; 
        this.coloumn1= str1 ;
        this.coloumn2= str2 ;
        this.coloumn3= str3 ;
   }
    public void insert(Object x, Object y, Object z, String s, Object ck) throws OutOfBoundsException, UnsupportedEncodingException, ParseException, DBAppException {

        if ( ((check(x,Bucket.getMinx())) < 0) || ((check(x,Bucket.getMaxx())) > 0)
                ||  ((check(y,Bucket.getMiny())) < 0)||  ((check(y,Bucket.getMaxy())) > 0)
                ||  ((check(z,Bucket.getMinz())) < 0) ||  ((check(z,Bucket.getMaxz())) > 0)) {
        	System.out.println(x);
        	System.out.println(Bucket.getMinx());
        	System.out.println(Bucket.getMaxx());
        	System.out.println(y);
        	System.out.println(Bucket.getMiny());
        	System.out.println(Bucket.getMaxy());
        	System.out.println(z);
        	System.out.println(Bucket.getMinz());
        	System.out.println(Bucket.getMaxz());
            throw new OutOfBoundsException("Insertion point is out of bounds! X: " + x + " Y: " + y + " Z: " + z );
        }
        Object midOfx ; 
        Object midOfy; 
        Object midOfz ;
        switch (x.getClass().getName()) {
 
        case "java.lang.String" :
        	 midOfx = getMiddleString(Bucket.getMaxx().toString(), Bucket.getMinx().toString());
             break;
        case "java.lang.Double" :
        	  midOfx = ((double)Bucket.getMaxx() + (double)Bucket.getMinx())/2;
        	  break;
        case "java.lang.Integer" :
        	 midOfx = ((Integer)Bucket.getMaxx() + (Integer)Bucket.getMinx())/2;
        	 break;
        default : 
        	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
        	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
            long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
            Date midwayDate = new Date(midwayTime);
        	midOfx = midwayDate ; 
        	 break;
        	
        }
        switch (y.getClass().getName()) {
        case "java.lang.String" :
        	 midOfy =  getMiddleString(Bucket.getMaxy().toString(), Bucket.getMiny().toString());
        	 break;
        case "java.lang.Double" :
        	  midOfy = ((double)Bucket.getMaxy() + (double)Bucket.getMiny())/2;
        	  break;
        case "java.lang.Integer" :
        	 midOfy = ((Integer)Bucket.getMaxy() + (Integer)Bucket.getMiny())/2;
        	 break;
        default : 
        	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
        	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
            long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
            Date midwayDate = new Date(midwayTime);
        	midOfy = midwayDate ; 
        	 break;
        	
        }
        switch (z.getClass().getName()) {
        case "java.lang.String" :
        	 midOfz = getMiddleString(Bucket.getMaxz().toString(), Bucket.getMinz().toString());
        	 break;
        case "java.lang.Double" :
        	  midOfz = ((double)Bucket.getMaxz() + (double)Bucket.getMinz())/2;
        	  break;
        case "java.lang.Integer" :
        	 midOfz = ((Integer)Bucket.getMaxz() + (Integer)Bucket.getMinz())/2;
        	 break;
        default : 
        	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
        	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
            long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
            Date midwayDate = new Date(midwayTime);
        	midOfz = midwayDate ; 
        	 break;
        }
        
        if (this.children[0] == null) {
        	if (Bucket.getCount()<Bucket.getSize()) {  //noteee
       		 // reference to record
       		
       		Vector <OctPoint> v = this.Bucket.getLocations() ;
       		OctPoint t = new OctPoint (x,y,z) ;
       		if(v.contains(t)) 
       		{
       			int index = v.indexOf(t);
       		    Bucket.getDuplicates().get(index).add(s+"_"+ck.toString());
       		}
       		else {
       		this.Bucket.setCount(this.Bucket.getCount()+1); 
       		this.Bucket.getBucket().add(s) ;
       		this.Bucket.getckeys().add(ck) ;
       		v.add(t) ;
       		this.Bucket.setLocations(v) ;} 
       	    }
        	
       	    else {
        
                  children[0] = new Octree((midOfx), (midOfy), (midOfz), this.Bucket.getMaxx(),this.Bucket.getMaxy(),this.Bucket.getMaxz(),this.coloumn1,this.coloumn2,this.coloumn3);
                  children[1] = new Octree((midOfx), (midOfy), Bucket.getMinz(), this.Bucket.getMaxx(),this.Bucket.getMaxy(),midOfz,this.coloumn1,this.coloumn2,this.coloumn3);
                  children[2] = new Octree((midOfx),Bucket.getMiny(),(midOfz),this.Bucket.getMaxx(),midOfy,this.Bucket.getMaxz(),this.coloumn1,this.coloumn2,this.coloumn3);
                  children[3] = new Octree((midOfx),Bucket.getMiny(),Bucket.getMinz(),this.Bucket.getMaxx(),midOfy,midOfz,this.coloumn1,this.coloumn2,this.coloumn3);
                  children[4] = new Octree(Bucket.getMinx(),(midOfy),(midOfz),midOfx, Bucket.getMaxy(),Bucket.getMaxz(),this.coloumn1,this.coloumn2,this.coloumn3);
                  children[5] = new Octree(Bucket.getMinx(),(midOfy),Bucket.getMinz(),midOfx,Bucket.getMaxy(),midOfz,this.coloumn1,this.coloumn2,this.coloumn3);
                  children[6] = new Octree(Bucket.getMinx(),Bucket.getMiny(),(midOfz),midOfx,midOfy,Bucket.getMaxz(),this.coloumn1,this.coloumn2,this.coloumn3);
                  children[7] = new Octree(Bucket.getMinx(),Bucket.getMiny(),Bucket.getMinz(),midOfx,midOfy,midOfz,this.coloumn1,this.coloumn2,this.coloumn3);
                  for (int i = 0  ; i< this.Bucket.getCount() ; i ++) {
               	   String s2 = this.Bucket.getBucket().get(i) ; 
               	   Object i2= this.Bucket.getckeys().get(i) ; 
               	   OctPoint t2 = this.Bucket.getLocations().get(i) ; 
               	   int octant = getOctant(t2,midOfx,midOfy,midOfz) ; 
               	   this.children[octant].insert(t2.getX(), t2.getY(), t2.getZ(), s2, i2);
                  }
                  
               
                  OctPoint t3 = new OctPoint (x,y,z) ; 
              	   int octant2 = getOctant(t3,midOfx,midOfy,midOfz) ; 
                  this.children[octant2].insert(x, y, z, s,ck);
       	}
        }
        else {
        OctPoint t3 = new OctPoint (x,y,z) ; 
        int octant2 = getOctant(t3,midOfx,midOfy,midOfz) ; 
        this.children[octant2].insert(x, y, z, s,ck); }
            
      
    }  //done
    public Object middle(Object x,Object y) throws ParseException
    {
    	Object result=null;
    	if(x instanceof Integer && y instanceof Integer)
    	{
    		int xx = (int)x;
    		int yy = (int)y;
    		result= (xx+yy)/2;
    	}
    	else if (x instanceof Double && y instanceof Double)
    	{
    		double xx = (double)x;
    		double yy = (double)y;
    		result= (xx+yy)/2;
    	}
    	else if (x instanceof Date && y instanceof Date)
    	{
    		Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(x.toString()) ; 
			Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(y.toString()) ;
			long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
			Date midwayDate = new Date(midwayTime);
			result = midwayDate ; 
			
    	}
    	else 
    	{
    		String xx= (String)x;
    		String yy=(String)y;
    		result=numberToAlphaString((alphaStringToNumber(xx)+alphaStringToNumber(yy))/2);
    	}
    	return result;
    }
    
    public boolean find(Object x, Object y, Object z) throws Exception{
		if( (check(x,this.Bucket.getMinx()) < 0) || (check(x,this.Bucket.getMaxx()) > 0)
				||  (check(y,this.Bucket.getMiny()) < 0)||  (check(y,this.Bucket.getMaxy()) > 0)
				||  (check(z,this.Bucket.getMinz()) < 0) ||  (check(z,this.Bucket.getMaxz()) > 0))
			return false;
		Object midx = middle(Bucket.getMaxx(),Bucket.getMinx()) ;
		Object midy = middle(Bucket.getMaxy(),Bucket.getMiny()) ;
		Object midz = middle(Bucket.getMaxz(),Bucket.getMinz()) ; 

		OctPoint oct = new OctPoint (x,y,z) ;  
		int pos = getOctant(oct,midx,midy,midz);
		Boolean b = false;

		if(children[0] != null) 
			return children[pos].find(x, y, z);
		if(Bucket.getCount()==0)
			return false;
		for (int i = 0 ; i < Bucket.getCount() ; i ++ ) {
			b = check(x,Bucket.getLocations().get(i).getX())==0 && check(y,Bucket.getLocations().get(i).getY())==0 && check(z,Bucket.getLocations().get(i).getZ())==0;
			if (b) 
				
				break ;
		}
		return b ;
	}
    public String get(Object x, Object y, Object z) throws UnsupportedEncodingException, ParseException{
    	if ( ((check(x,Bucket.getMinx())) < 0) || ((check(x,Bucket.getMaxx())) > 0)
                ||  ((check(y,Bucket.getMiny())) < 0)||  ((check(y,Bucket.getMaxy())) > 0)
                ||  ((check(z,Bucket.getMinz())) < 0) ||  ((check(z,Bucket.getMaxz())) > 0))
	  		     return null;
       Object midx ; 
       Object midy; 
       Object midz ; 
       switch (x.getClass().getName()) {
	  
       case "java.lang.String" :
    		  midx = getMiddleString(Bucket.getMaxx().toString(), Bucket.getMinx().toString());
    	       break;
    	  case "java.lang.Double" :
    	  	  midx = ((double)Bucket.getMaxx() + (double)Bucket.getMinx())/2;
    	  	  break;
    	  case "java.lang.Integer" :
    	  	 midx = ((Integer)Bucket.getMaxx() + (Integer)Bucket.getMinx())/2;
    	  	 break;
    	  default : 
    		  Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
    	  	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
    	      long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
    	      Date midwayDate = new Date(midwayTime);
    	  	midx = midwayDate ; 
    	  	 break;
  	
        }
        switch (y.getClass().getName()) {
        case "java.lang.String" :
        	  midy = getMiddleString(Bucket.getMaxy().toString(), Bucket.getMiny().toString());
        	 break;
        case "java.lang.Double" :
        	  midy = ((double)Bucket.getMaxy() + (double)Bucket.getMiny())/2;
        	  break;
        case "java.lang.Integer" :
        	 midy = ((Integer)Bucket.getMaxy() + (Integer)Bucket.getMiny())/2;
        	 break;
        default : 
      	  Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
        	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
            long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
            Date midwayDate = new Date(midwayTime);
        	midy = midwayDate ; 
        	 break;
  	
       }
        switch (z.getClass().getName()) {
        case "java.lang.String" :
      	  midz = getMiddleString(Bucket.getMaxz().toString(), Bucket.getMinz().toString());
        	 break;
        case "java.lang.Double" :
        	  midz = ((double)Bucket.getMaxz() + (double)Bucket.getMinz())/2;
        	  break;
        case "java.lang.Integer" :
        	 midz = ((Integer)Bucket.getMaxz() + (Integer)Bucket.getMinz())/2;
        	 break;
        default : 
      	  Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
        	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
            long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
            Date midwayDate = new Date(midwayTime);
        	midz = midwayDate ; 
        	 break;
        }
       
        OctPoint oct = new OctPoint (x,y,z) ;  
        int pos = getOctant(oct,midx,midy,midz);
        Boolean b = false;

        if(children[0] != null)
            return children[pos].get(x, y, z);
        if(Bucket.getCount()==0)
            return null;
      for (int i = 0 ; i < Bucket.getCount() ; i ++ ) {
    	  b = check(x,Bucket.getLocations().get(i).getX())==0 && check(y,Bucket.getLocations().get(i).getY())==0 && check(z,Bucket.getLocations().get(i).getZ())==0;
        if (b) 
      	  return Bucket.getBucket().get(i) + "," + Bucket.getckeys().get(i); //could be ,
      }
        return null;
}

    public boolean remove(Object x, Object y, Object z) throws UnsupportedEncodingException, ParseException{
    	if ( ((check(x,Bucket.getMinx())) < 0) || ((check(x,Bucket.getMaxx())) > 0)
                ||  ((check(y,Bucket.getMiny())) < 0)||  ((check(y,Bucket.getMaxy())) > 0)
                ||  ((check(z,Bucket.getMinz())) < 0) ||  ((check(z,Bucket.getMaxz())) > 0))
    	  		return false;
    Object midx ; 
    Object midy; 
    Object midz ; 
    switch (x.getClass().getName()) {
    	  
    case "java.lang.String" :
    	 midx = getMiddleString(Bucket.getMaxx().toString(), Bucket.getMinx().toString());
         break;
    case "java.lang.Double" :
    	  midx = ((double)Bucket.getMaxx() + (double)Bucket.getMinx())/2;
    	  break;
    case "java.lang.Integer" :
    	 midx = ((Integer)Bucket.getMaxx() + (Integer)Bucket.getMinx())/2;
    	 break;
    default : 
    	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
    	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
        long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
        Date midwayDate = new Date(midwayTime);
    	midx = midwayDate ; 
    	 break;
    }
    switch (y.getClass().getName()) {
    case "java.lang.String" :
    	 midy = getMiddleString(Bucket.getMaxy().toString(), Bucket.getMiny().toString());
    	 break;
    case "java.lang.Double" :
    	  midy = ((double)Bucket.getMaxy() + (double)Bucket.getMiny())/2;
    	  break;
    case "java.lang.Integer" :
    	 midy = ((Integer)Bucket.getMaxy() + (Integer)Bucket.getMiny())/2;
    	 break;
    default : 
    	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
    	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
        long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
        Date midwayDate = new Date(midwayTime);
    	midy = midwayDate ; 
    	 break;
    }
    switch (z.getClass().getName()) {
    case "java.lang.String" :
    	 midz = getMiddleString(Bucket.getMaxz().toString(), Bucket.getMinz().toString());
    	 break;
    case "java.lang.Double" :
    	  midz = ((double)Bucket.getMaxz() + (double)Bucket.getMinz())/2;
    	  break;
    case "java.lang.Integer" :
    	 midz = ((Integer)Bucket.getMaxz() + (Integer)Bucket.getMinz())/2;
    	 break;
    default : 
    	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
    	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
        long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
        Date midwayDate = new Date(midwayTime);
    	midz = midwayDate ; 
    	 break;
    }

    OctPoint oct = new OctPoint (x,y,z) ;  
    int pos = getOctant(oct,midx,midy,midz);
    Boolean b = false;
      if(children[0] != null)
          return children[pos].remove(x, y, z);
      if(Bucket.getCount()==0)
          return false;
    for (int i = 0 ; i < Bucket.getCount() ; i ++ ) {
    	b = check(x,Bucket.getLocations().get(i).getX())==0 && check(y,Bucket.getLocations().get(i).getY())==0 && check(z,Bucket.getLocations().get(i).getZ())==0;
    if (b) {
    	Bucket.getLocations().remove(i) ; 
    	Bucket.getckeys().remove(i) ; 
    	Bucket.getBucket().remove(i) ; 
    	Bucket.setCount(Bucket.getCount()-1);
    			} } 
      return false;
    }
    public boolean remove2(Object x, Object y, Object z, String filepath, Object ck) throws UnsupportedEncodingException, ParseException{
    	if ( ((check(x,Bucket.getMinx())) < 0) || ((check(x,Bucket.getMaxx())) > 0)
                ||  ((check(y,Bucket.getMiny())) < 0)||  ((check(y,Bucket.getMaxy())) > 0)
                ||  ((check(z,Bucket.getMinz())) < 0) ||  ((check(z,Bucket.getMaxz())) > 0))
    	  		return false;
    Object midx ; 
    Object midy; 
    Object midz ; 
    switch (x.getClass().getName()) {
    	  
    case "java.lang.String" :
    	 midx = getMiddleString(Bucket.getMaxx().toString(), Bucket.getMinx().toString());
         break;
    case "java.lang.Double" :
    	  midx = ((double)Bucket.getMaxx() + (double)Bucket.getMinx())/2;
    	  break;
    case "java.lang.Integer" :
    	 midx = ((Integer)Bucket.getMaxx() + (Integer)Bucket.getMinx())/2;
    	 break;
    default : 
    	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
    	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
        long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
        Date midwayDate = new Date(midwayTime);
    	midx = midwayDate ; 
    	 break;
    }
    switch (y.getClass().getName()) {
    case "java.lang.String" :
    	 midy = getMiddleString(Bucket.getMaxy().toString(), Bucket.getMiny().toString());
    	 break;
    case "java.lang.Double" :
    	  midy = ((double)Bucket.getMaxy() + (double)Bucket.getMiny())/2;
    	  break;
    case "java.lang.Integer" :
    	 midy = ((Integer)Bucket.getMaxy() + (Integer)Bucket.getMiny())/2;
    	 break;
    default : 
    	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
    	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
        long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
        Date midwayDate = new Date(midwayTime);
    	midy = midwayDate ; 
    	 break;
    }
    switch (z.getClass().getName()) {
    case "java.lang.String" :
    	 midz = getMiddleString(Bucket.getMaxz().toString(), Bucket.getMinz().toString());
    	 break;
    case "java.lang.Double" :
    	  midz = ((double)Bucket.getMaxz() + (double)Bucket.getMinz())/2;
    	  break;
    case "java.lang.Integer" :
    	 midz = ((Integer)Bucket.getMaxz() + (Integer)Bucket.getMinz())/2;
    	 break;
    default : 
    	Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMaxx().toString()) ; 
    	Date enddate =  new SimpleDateFormat("yyyy-MM-dd").parse(Bucket.getMinx().toString()) ;
        long midwayTime = (startdate.getTime() + enddate.getTime()) / 2;
        Date midwayDate = new Date(midwayTime);
    	midz = midwayDate ; 
    	 break;
    }

    OctPoint oct = new OctPoint (x,y,z) ;  
    int pos = getOctant(oct,midx,midy,midz);
    Boolean b = false;
      if(children[0] != null)
          return children[pos].remove2(x, y, z,filepath,ck);
      if(Bucket.getCount()==0)
          return false;
    for (int i = 0 ; i < Bucket.getCount() ; i ++ ) {
    b = filepath.equals(Bucket.getBucket().get(i)) && check(ck,Bucket.getckeys().get(i))==0 && check(x,Bucket.getLocations().get(i).getX())==0 && check(y,Bucket.getLocations().get(i).getY())==0 && check(z,Bucket.getLocations().get(i).getZ())==0;
    if (b) {
    	Bucket.getLocations().remove(i) ; 
    	Bucket.getckeys().remove(i) ; 
    	Bucket.getBucket().remove(i) ; 
    	Bucket.setCount(Bucket.getCount()-1);
    			} } 
      return false;
    }

    private static  int  getOctant (OctPoint p , Object midx, Object midy , Object midz ) {
    	int pos;

        if((check(p.getX(),midx))<0){
            if((check(p.getY(),midy))<0){
                if(((check(p.getZ(),midz))<0))

                    pos = 7;
                else
                    pos = 6;
            }else{
                if((check(p.getZ(),midz))<0)
                    pos = 5;
                else
                    pos = 4;
            }
        }else{
            if(((check(p.getY(),midy))<0)){
                if((check(p.getZ(),midz))<0)
                    pos = 3;
                else
                    pos = 2;
            }else {
                if((check(p.getZ(),midz))<0)
                    pos = 1;
                else
                    pos = 0;
            }
        }
        return pos ; 
    }
    public static String getMiddleString(String s, String t)
    {
        return numberToAlphaString((alphaStringToNumber(s)+alphaStringToNumber(t))/2);
    }

    public static long alphaStringToNumber(String str) {
        long n = 0;
        for (char elem : str.toCharArray()) {
            n *= 26;
            n += 1 + (elem - 'a');
        }
        return n;
    }

    public static String numberToAlphaString(long n) {
        StringBuilder r = new StringBuilder();
        while (n > 0) {
            r.append((char)('a' + ( --n % 26) ));
            n /= 26;
        }
        return r.reverse().toString();
    }
    
    public Bucket getBucket() {
		return Bucket;
	}
	public void setBucket(Bucket bucket) {
		Bucket = bucket;
	}
	public Octree<T>[] getChildren() {
		return children;
	}
	public void setChildren(Octree<T>[] children) {
		this.children = children;
	}
	public T getObject() {
		return object;
	}
	public void setObject(T object) {
		this.object = object;
	}
	public String getColoumn1() {
		return coloumn1;
	}
	public void setColoumn1(String coloumn1) {
		this.coloumn1 = coloumn1;
	}
	public String getColoumn2() {
		return coloumn2;
	}
	public void setColoumn2(String coloumn2) {
		this.coloumn2 = coloumn2;
	}
	public String getColoumn3() {
		return coloumn3;
	}
	public void setColoumn3(String coloumn3) {
		this.coloumn3 = coloumn3;
	}  
	
	public static int check(Object o1,Object o2) {
		if(o1 instanceof Integer && o2 instanceof Integer ) {
			if((int)o1>(int)o2) {
				return 1;
			}
			else if ((int)o1<(int)o2) {
				return -1;
			}
			else {
				return 0;
			}
		}
		else if(o1 instanceof String && o2 instanceof String) {
			return ((String)o1).compareTo((String)o2);
		}
		else if(o1 instanceof Double && o2 instanceof Double) {
			if((Double)o1>(Double)o2) {
				return 1;
			}
			else if ((Double)o1<(Double)o2){
				return -1;
			}
			else {
				return 0;
			}
		}
		else if(o1 instanceof Date && o2 instanceof Date) {
			return ((Date)o1).compareTo((Date)o2);
		}  
		return 1;
	}
	
	public void displayNode(Octree node, int depth) {
		StringBuilder indent = new StringBuilder();
		for (int i = 0; i < depth; i++) {
			indent.append("\t");
		}
		Boolean b = node.children[0] == null ; 
		System.out.println(indent.toString() + "Node (Leaf: " + b + ")");
		if (node.getBucket()!=null) {
			System.out.println(indent.toString() + "Bounds: " + node.getBucket().displaybounds());}
		else { 
			System.out.println(); 
		}

		if (!b) {
			for (int i = 0; i < 8; i++) {
				System.out.println(indent.toString() + "Child " + i + ":");
				displayNode(node.children[i], depth + 1);
			}
		} else {
			System.out.println(indent.toString() + "Entries:");
			for (int i= 0 ; i< node.Bucket.getCount() ; i++) {
				System.out.println(indent.toString() + "\t" + node.getBucket().getBucket().get(i) + "@" +node.getBucket().getLocations().get(i).display()) ;

			}
			if(node.getBucket().getDuplicates().size() != 0) {
			for (int i= 0 ; i< node.Bucket.getCount() ; i++) { 
				int x = i+1 ;
				System.out.println(indent.toString() + "duplicates of entry " + "" + x + ":");
				    for (int j = 0 ; j < node.getBucket().getDuplicates().get(i).size() ; j ++ ) {

					     System.out.println(indent.toString() + "\t" + node.getBucket().getDuplicates().get(i).get(j) ); 
				     }
				}
			}}
	}}