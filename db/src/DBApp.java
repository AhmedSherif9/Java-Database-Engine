  import java.io.BufferedReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.xml.crypto.Data;
import java.io.Serializable;

public class DBApp implements Serializable {
	public void init( ) {}
	
	public void createTable(String strTableName, 
			 String strClusteringKeyColumn, 
			Hashtable<String,String> htblColNameType, 
			Hashtable<String,String> htblColNameMin, 
			Hashtable<String,String> htblColNameMax ) throws DBAppException{
		 for (String col : htblColNameType.keySet())
	            if (!htblColNameMin.containsKey(col) || !htblColNameMax.containsKey(col))
	                throw new DBAppException();
		 for (String col : htblColNameMax.keySet())
	            if (!htblColNameType.containsKey(col) )
	                throw new DBAppException();

	        for (String col : htblColNameMin.keySet())
	            if (!htblColNameType.containsKey(col) )
	                throw new DBAppException();

	        for (String col : htblColNameType.keySet())
	            if (htblColNameType.get(col).equals("java.lang.Integer") || htblColNameType.get(col).equals("java.lang.String")|| htblColNameType.get(col).equals("java.lang.Double")|| htblColNameType.get(col).equals("java.util.Date")) {
	            	
	            }
	            else {
	                throw new DBAppException();
	            }
		File newTable= new File("src/resources/"+strTableName); //3aaaa
		if (newTable.exists())
			throw new DBAppException("Table already exists!");
		else
			newTable.mkdir();
		Vector <String> page= new Vector<String>();
		Table table =new Table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax,page);
		try {
		FileOutputStream fileOut = new FileOutputStream("src/resources/"+strTableName+"/"+strTableName+".ser");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(table);
		out.close();
		fileOut.close();}
		catch (IOException e) {
           e.printStackTrace();
       }
	
		try {
			FileReader currentFile = new FileReader("src/resources/metadata.csv");
			BufferedReader br = new BufferedReader(currentFile);
			StringBuilder stringBuilder = new StringBuilder();
			String line;
           while ((line = br.readLine()) != null) {
           	stringBuilder.append(line).append('\n');
           	
                                                   }
           FileWriter metaDataFile = new FileWriter("src/resources/metadata.csv");
              for(String ColName :htblColNameType.keySet()) {
           	   stringBuilder.append(strTableName).append(",");
           	   stringBuilder.append(ColName).append(",");
           	   stringBuilder.append(htblColNameType.get(ColName)).append(",");
           	   if (ColName.equals(strClusteringKeyColumn))
           		   stringBuilder.append("TRUE").append(",");
           	   else 
           		   stringBuilder.append("FALSE").append(",");
           		   
           	   stringBuilder.append("null").append(",");
           	   stringBuilder.append("null").append(",");
           	   stringBuilder.append(htblColNameMin.get(ColName)).append(",");
                 stringBuilder.append(htblColNameMax.get(ColName));
                  stringBuilder.append("\n");
              }
              metaDataFile.write(stringBuilder.toString());
              metaDataFile.close();	               

		}
		catch (IOException ignored) {
       }
	}
	public boolean checkIfTableExists(String tableName)
	{
		boolean flag=false;
		try {
			String file= "src/resources/metadata.csv";
			FileReader reader=new FileReader(file);
			BufferedReader buff=new BufferedReader(reader);
			String line=buff.readLine();
			
			while(!(line==null)) {
				String arr[]=line.split(",");
				if(tableName.equals(arr[0])) {
					flag=true;
					break;
				}
				line=buff.readLine();
			}

	}catch(Exception e){
		System.out.println("Couldn't open csv file");
		
	}
		return flag;
		}
	//check that all the attributes' names are valid 
	public boolean allOfTheAttributes(String []columns,String tableName)
	{
		int count =0;
		boolean flag = false;
		try {
			for (int i=0;i<columns.length;i++)
			{
				String file= "src/resources/metadata.csv";
				FileReader reader=new FileReader(file);
				BufferedReader buff=new BufferedReader(reader);
				String line=buff.readLine();
				while(line!=null) {
					String arr[]=line.split(",");
				    if(columns[i].equals(arr[1]) && arr[0].equals(tableName))
				    {
					     count++;
					     break;
				    }
				    line=buff.readLine();
			     }
		    }	
			System.out.println(count);
			

	}catch(Exception e){
		System.out.println("Couldn't open csv file");
		
	}
		if(count==3) {
			flag=true;}
		return flag;
		
	}
	
	
	//getting min max of each attribute and overriding
	public void gettingMinMaxAndOverridingInTheCSV(String []columns,String tableName) throws IOException 
	{
		String [] output =new String[6];
        // try {
        String file ="src/resources/metadata.csv";
        FileReader reader = new FileReader(file);
        BufferedReader buff = new BufferedReader(reader);
        StringBuilder s = new StringBuilder();
        String line = buff.readLine();
        String indexName = columns[0] + "" + columns[1] + "" + columns[2] + "Index";
        String indexType = "Octree";
        FileWriter finalOne = new FileWriter(file);
        int count=0;
        do {
            boolean inserted = false;
            
            String arr[] = line.split(",");
            for (int i = 0; i < columns.length; i++) {
 
                if (columns[i].equals(arr[1]) && arr[0].equals(tableName)) {
					count+=1;
                    s.append(arr[0]).append(",");
                    s.append(arr[1]).append(",");
                    s.append(arr[2]).append(",");
                    s.append(arr[3]).append(",");
                    s.append(indexName).append(",");
                    s.append(indexType).append(",");
                    s.append(arr[6]).append(",");
                    s.append(arr[7]).append('\n');
                    inserted = true;
                    break;

                }

            }

            if (!inserted) {
                s.append(line).append('\n');

            }

        } while ((line = buff.readLine()) != null);

        finalOne.write(s.toString());
        finalOne.close();

        // } catch (Exception e) {
        // System.out.println("Couldn't open csv file");

        // }


    }
	public Object[] getObjects(Record s ,String[]columns)
	{
		Object[] output= new Object[4];
	  Hashtable<String, Object> values=s.colNameValue;
	  output[0]=values.get(columns[0]);
	  output[1]=values.get(columns[1]);
	  output[2]=values.get(columns[2]);
	  output[3]=s.getClustringKey();
	  return output;
	  
		
	}
	public Hashtable<String,Object> getMin(String Tablename,String[]x) throws IOException, ParseException
	{
		    Hashtable <String,Object>output=new Hashtable<String,Object>();
	        for(int i=0;i<x.length;i++)
	        {
	        	String file= "src/resources/metadata.csv";
				FileReader reader=new FileReader(file);
				BufferedReader buff=new BufferedReader(reader);
				String line=buff.readLine();
	        	while(line != null)
	        	 {
	        		 String arr[] = line.split(",");
	        		 if(x[i].equals(arr[1]) && arr[0].equals(Tablename))
	        		 {
	        			 Object o = stringType(arr[2],arr[6]);
	        			 output.put(x[i], o);
	        			 break;
	        		 }
	        	     line= buff.readLine();
	        	 }
	        	
	        	
	        }
	        return output;
	        
	}
	
	public static Object stringType(String type,String value) throws ParseException {
		Object o;
		if(type.equals("java.lang.Integer")) {
        	o = Integer.parseInt(value);
        }
        else if(type.equals("java.lang.String")) {
        	o = value;;
        }
        else if(type.equals("java.lang.Double")) {
        	o= Double.parseDouble(value);
        }
        else {
        	Date date = (Date) new SimpleDateFormat("yyyy-MM-dd").parse(value);
        	o=date;
        }
		return o;
	}
	
	public Hashtable<String,Object> getMax(String Tablename,String[]x) throws IOException, ParseException
	{
		    Hashtable <String,Object>output=new Hashtable<String,Object>();
	        for(int i=0;i<x.length;i++)
	        {
	        	String file= "src/resources/metadata.csv";
				FileReader reader=new FileReader(file);
				BufferedReader buff=new BufferedReader(reader);
				String line=buff.readLine();
	        	while(line != null)
	        	 {
	        		 String arr[] = line.split(",");
	        		 if(x[i].equals(arr[1]) && arr[0].equals(Tablename))
	        		 {
	        			 Object o = stringType(arr[2],arr[7]);
	        			 output.put(x[i], o);
	        			 break;
	        		 }
	        	     line= buff.readLine();
	        	 }
	        	
	        	
	        }
	        return output;
	        
	}

	
	public void createIndex(String strTableName, 
			String[] strarrColName) throws DBAppException {
		if (!checkIfTableExists(strTableName))
		{
			System.out.println("can't create an index on a table that doesn't exist");
		}
		else 
		{
			if(!allOfTheAttributes(strarrColName,strTableName)) 
			{
				System.out.println("to create an Octree index yesssssssss must enter 3 valid attributes'name on the given tableName ");
			}
			else 
			{
				try 
				{
					Table t = null;
					FileInputStream filein = new FileInputStream("src/resources/" + strTableName +"/" +strTableName+".ser");
					ObjectInputStream in = new ObjectInputStream(filein);
					t = (Table) in.readObject();
					in.close();
					filein.close();
					//call helper bta3t al override
					gettingMinMaxAndOverridingInTheCSV(strarrColName,strTableName);
					Hashtable<String,Object>max= getMax(strTableName,strarrColName);
					Hashtable<String,Object>min= getMin(strTableName,strarrColName);
					if(t.getPages().size()==0) 
					{
						Octree index= new Octree(min.get(strarrColName[0]),min.get(strarrColName[1]),min.get(strarrColName[2]),max.get(strarrColName[0]),max.get(strarrColName[1]),max.get(strarrColName[2]),strarrColName[0],strarrColName[1],strarrColName[2]);
						  String filename = "src/resources/" + strTableName +"/"+strTableName +""+strarrColName[0]+""+strarrColName[1]+""+strarrColName[2]+"Index"+".ser";
							try {
								FileOutputStream fileOut = new FileOutputStream(filename);
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(index);
								out.close();
								fileOut.close();}
								catch (IOException e) {
						           System.out.println("file couldn't be created");
						           e.printStackTrace();
						       }
					}
					else
					{
						Octree index= new Octree(min.get(strarrColName[0]),min.get(strarrColName[1]),min.get(strarrColName[2]),max.get(strarrColName[0]),max.get(strarrColName[1]),max.get(strarrColName[2]),strarrColName[0],strarrColName[1],strarrColName[2]);
						Vector pages= t.pages;
						
						for(int i =0;i<t.pages.size();i++)
						{
							String pagePath=(String) pages.get(i);
							Page p = getPage(t.pages.get(i));
							for(int j=0;j<p.getRecords().size();j++)
							{
								Record r=p.getRecords().get(j);
								Object[] output= getObjects(r ,strarrColName);
								index.insert(output[0], output[1], output[2],pagePath , output[3]);
							}
							releasePage(pagePath,p);
							
						}
						File f=new File("src/resources/"+strTableName+"/"+strTableName+".ser"); 
						f.delete();
						FileOutputStream tableOut = new FileOutputStream("src/resources/"+strTableName+"/"+strTableName+".ser");
						ObjectOutputStream outable = new ObjectOutputStream(tableOut);
						outable.writeObject(t);
						outable.close();
						tableOut.close();
						  String filename = "src/resources/" + strTableName +"/"+strTableName +""+strarrColName[0]+""+strarrColName[1]+""+strarrColName[2]+"Index"+".ser";
					        try (FileOutputStream fos = new FileOutputStream(filename);
					             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
					            oos.writeObject(index);
					        } catch (IOException e) {
					            System.out.println("couldn't create file's index");
					        }
						
						
					}
					
					
				}
				catch(Exception e)
				{
					//System.out.println("can't deserialize table");
					e.printStackTrace();
				}
			
		}
			}}
		
		
		
	public void insertIntoTable(String strTableName,  //a single row insertion 
		 Hashtable<String,Object> htblColNameValue) 
		 throws DBAppException{
		try {
			String file= "src/resources/metadata.csv";
			FileReader reader=new FileReader(file);
			BufferedReader buff=new BufferedReader(reader);
			String line=buff.readLine();
			boolean flag1=false;
			while(!(line==null)) {
				String arr[]=line.split(",");
				if(strTableName.equals(arr[0])) {
					flag1=true;
					break;
				}
				line=buff.readLine();
			}
			if(flag1=false) {
				throw new DBAppException("Table "+strTableName+" does not exist");
			}
		    String clskey=null;
			for(String colname : htblColNameValue.keySet()) {
				FileReader reader2=new FileReader(file);
				BufferedReader buff2=new BufferedReader(reader2);
				String line2=buff2.readLine();
				boolean flag2=false;
		    	while(!(line2==null)) {
		    		String arr[]=line2.split(",");
		    		if(strTableName.equals(arr[0]) && colname.equals(arr[1])) {
		    			flag2=true;
		    			if(arr[3].equals("TRUE")) {
		    				clskey=colname;
		    			}
		    			if(arr[2].equals("java.lang.Integer")) {
		    				if(!(htblColNameValue.get(colname) instanceof Integer)) {
		    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type Integer");
		    				}	
		    				if(((int)htblColNameValue.get(colname))<Integer.parseInt(arr[6])
		    				|| ((int)htblColNameValue.get(colname))>Integer.parseInt(arr[7])) {
		    				    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
		    				}
		    			}
		    			else if(arr[2].equals("java.lang.String")){
		    				if(!(htblColNameValue.get(colname) instanceof String)) {
		    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type String");
		    				}	
                            if(((String)htblColNameValue.get(colname)).compareTo(arr[6])<0 
                            || ((String)htblColNameValue.get(colname)).compareTo(arr[7])>0)  {
                        	    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
		    				}
		    			}
		    			else if(arr[2].equals("java.lang.Double")) {
		    				if(!(htblColNameValue.get(colname) instanceof Double)) {
		    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type Double");
		    				}	
		    				if(((Double)htblColNameValue.get(colname))<Double.parseDouble(arr[6])
				    		|| ((Double)htblColNameValue.get(colname))>Double.parseDouble(arr[7])) {
		    				    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
				   			}
		    			}
		    			else if (arr[2].equals("java.util.Date")) {
		    				if(!(htblColNameValue.get(colname) instanceof Date)) {
		    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type Date");
		    				}
		    				Date min = (Date) new SimpleDateFormat("yyyy-MM-dd").parse(arr[6]);
		    				Date max = (Date) new SimpleDateFormat("yyyy-MM-dd").parse(arr[7]);
		    				if((((Date)htblColNameValue.get(colname)).compareTo(min)<0)
						   	|| (((Date)htblColNameValue.get(colname)).compareTo(max)>0)) {
				    		    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
							}
		    			} 
		    			break;
		    		}
		    		line2=buff2.readLine();
		    	}
		    	if(flag2==false) {
		    		throw new DBAppException("column "+colname+" does not exist in Table");
		    	}
		    }
			if(clskey==null) {
				throw new DBAppException("clustering key is not inserted in the record");
			}
			
			
			Record r=new Record(strTableName,htblColNameValue,clskey);
			Table table= null;
			FileInputStream filein = new FileInputStream("src/resources/"+strTableName+"/"+strTableName+".ser");
			ObjectInputStream in = new ObjectInputStream(filein);
			table = (Table) in.readObject();
			in.close();
			filein.close();
		    String path="";
			if(table.pages.isEmpty()) {
				Vector <Record> records = new Vector<Record>();
				records.add(r);
				Page p=new Page(strTableName,clskey,1,records);
				table.pages.add(0,"src/resources/"+strTableName+"/"+p.tableName+"_0.ser");
				path="src/resources/"+strTableName+"/"+p.tableName+"_0.ser";
				FileOutputStream tableOut = new FileOutputStream(table.pages.get(0));
			    ObjectOutputStream outable = new ObjectOutputStream(tableOut);
				outable.writeObject(p);
				outable.close();
				tableOut.close();
				}
			else {
				int pageindex=binarysearchpage(table.pages,r.getClustringKey()); //note
				path="src/resources/"+strTableName+"/"+strTableName+"_"+pageindex+".ser";
				if(pageindex==table.pages.size()) {
				//	System.out.println("well,yesssssssss yesssssssss");
				     Vector <Record> records = new Vector<Record>();
				     records.add(r);
				     int pagenum=pageindex+1;
				     Page p=new Page(strTableName,clskey,pagenum,records);
				     table.pages.add(pageindex,"src/resources/"+strTableName+"/"+p.tableName+"_"+pageindex+".ser");
				     FileOutputStream fileOut = new FileOutputStream("src/resources/"+strTableName+"/"+p.tableName+"_"+pageindex+".ser");
				     ObjectOutputStream out = new ObjectOutputStream(fileOut);
				     out.writeObject(p);
				     out.close();
				     fileOut.close();
				}
				else {
					 Page page = getPage(table.pages.get(pageindex));
				     int recordindex;
				     if (binarySearch(page.getClustringKeys(), r.getClustringKey()) == -1) {
					     recordindex=binarySearchInsertion(page.getClustringKeys(),r.clustringKey);
				     } else { 
					     throw new DBAppException("The clustring Key Already exists!");
				     }
				     page.records.add(recordindex,r);
				     page.updateMinMaxCurrent();
				     if(page.records.size()>page.maximumRowsCountinPage) { //page was full
					     Record temporary= page.records.get(page.maximumRowsCountinPage);
					     page.records.remove(page.maximumRowsCountinPage);
					     page.updateMinMaxCurrent();
					     releasePage(table.pages.get(pageindex),page);
					     boolean emptyexists = false;
					     Page pagee;FileInputStream fil;ObjectInputStream obj;
					     while((++pageindex)<table.pages.size()) {
						     pagee=getPage(table.pages.get(pageindex));
						     int rr;
						     if (binarySearch(pagee.getClustringKeys(), r.getClustringKey()) == -1) {
							     rr=binarySearchInsertion(pagee.getClustringKeys(),r.clustringKey);
						     } else { 
							     throw new DBAppException("The clustring Key Already exists!");
						     }
						     if(!(pagee.isFull())) {
							     emptyexists=true;
							     pagee.records.add(rr,temporary);
							     pagee.updateMinMaxCurrent();
							     releasePage(table.pages.get(pageindex),pagee);
							     break;
						     }
						     pagee.records.add(rr,temporary);
						     temporary= pagee.records.get(pagee.maximumRowsCountinPage);
						     pagee.records.remove(pagee.maximumRowsCountinPage);
						     pagee.updateMinMaxCurrent();
						     releasePage(table.pages.get(pageindex),pagee);
					     }
					     if(emptyexists==false) {
						     Vector<Record> finalrecords=new Vector<Record>();
						     finalrecords.add(temporary);
						     Page finalone=new Page(strTableName,clskey,table.pages.size()+1,finalrecords);
						     table.pages.add(pageindex,"src/resources/"+strTableName+"/"+finalone.tableName+"_"+pageindex+".ser");
						     FileOutputStream tableOut = new FileOutputStream(table.pages.get(pageindex));
						     ObjectOutputStream outable = new ObjectOutputStream(tableOut);
							 outable.writeObject(finalone);
							 outable.close();
							 tableOut.close();
					     }
				     }
				     else {
				    	 releasePage(table.pages.get(pageindex),page);
				     }
	             }
			}
			File f=new File("src/resources/"+strTableName+"/"+strTableName+".ser"); 
			f.delete();
			FileOutputStream tableOut = new FileOutputStream("src/resources/"+strTableName+"/"+strTableName+".ser");
			ObjectOutputStream outable = new ObjectOutputStream(tableOut);
			outable.writeObject(table);
			outable.close();
			tableOut.close();
			
			//index
			
			Vector <String> indecies = getindecies(strTableName, file);
			for(int i=0;i<indecies.size();i++) {
			//	System.out.println("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser");
				Octree t = getOctree("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser");
				Object o1 = htblColNameValue.get(t.getColoumn1());
				Object o2 = htblColNameValue.get(t.getColoumn2());
				Object o3 = htblColNameValue.get(t.getColoumn3());
				t.insert(o1, o2, o3, path, r.clustringKey);
				releaseOctree("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser",t);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Vector<String> getindecies(String strTableName,String file) throws IOException{
		Vector <String> indecies = new Vector<String>();
		FileReader readeri=new FileReader(file);
		BufferedReader buffi=new BufferedReader(readeri);
		String linei=buffi.readLine();
		while(!(linei == null)) {
			String[] arr= linei.split(",");
			if(arr[0].equals(strTableName) && !(arr[4].equals("null"))) {
				if(!(indecies.contains(arr[4]))) {
				    indecies.add(arr[4]);
				}
			}
			linei=buffi.readLine();
		}
		return indecies;
	}
	
	public Vector<String> indexExists(String tableName, Hashtable<String, Object> s) {
		Vector<String> result = new Vector<String>();
		if (s.size() < 3) {
			result.add("noIndex");
			return result;
		} else {
			String file = "src/resources/metadata.csv";
			Vector<String> indecies = new Vector<String>();
			FileReader readeri;
			try {
				readeri = new FileReader(file);
				BufferedReader buffi = new BufferedReader(readeri);
				String linei =null;
				while ((linei = buffi.readLine()) != null) {
					String[] arr = linei.split(",");
					if (arr[0].equals(tableName) && !(arr[4].equals("null"))) {
						if (!(indecies.contains(arr[4]))) {
							indecies.add(arr[4]);
						}
					}
				}
				for (int i = 0; i < indecies.size(); i++) {
					try {

						String path = "src/resources/" + tableName + "/" + tableName + indecies.get(i) + ".ser";
						Octree x = getOctree(path);
						if (s.containsKey(x.getColoumn1()) && s.containsKey(x.getColoumn2())
								&& s.containsKey(x.getColoumn3())) {
							result.add(indecies.get(i));
						}
						releaseOctree(path, x);
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						System.out.println("couldn't desrialize the index");
					}
				}

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;

	}
	public static Page getPage(String filepath) throws IOException, ClassNotFoundException {
		 Page page=null;
	     FileInputStream filin = new FileInputStream(filepath);
	     ObjectInputStream objin = new ObjectInputStream(filin);
	     page = (Page) objin.readObject();
	     objin.close();
	     filin.close();
	     return page;
	}
	
	public static void releasePage(String filepath,Page page) throws IOException {
		File f=new File(filepath); 
		f.delete();
		FileOutputStream tableOut = new FileOutputStream(filepath);
		ObjectOutputStream outable = new ObjectOutputStream(tableOut);
		outable.writeObject(page);
		outable.close();
		tableOut.close();
	}	
	public static void releaseOctree(String filepath,Octree tree) throws IOException {
		File f=new File(filepath); 
		f.delete();
		FileOutputStream tableOut = new FileOutputStream(filepath);
		ObjectOutputStream outable = new ObjectOutputStream(tableOut);
		outable.writeObject(tree);
		outable.close();
		tableOut.close();
	}	
	
	public static Octree getOctree(String filepath) throws IOException, ClassNotFoundException {
	     FileInputStream filin = new FileInputStream(filepath);
	     ObjectInputStream objin = new ObjectInputStream(filin);
	    Octree t = (Octree) objin.readObject();
	     objin.close();
	     filin.close();
	     return t;
	}
		
	public static int binarySearch(Vector<Object> arr, Object x) {
		int l = 0, r = arr.size() - 1;
		// int m = 0;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if ((check(x,arr.get(m))==0))
				return m;

			// If x greater, ignore left half
			if (check(x,arr.get(m)) > 0)
				l = m + 1;

			// If x is smaller, ignore right half
			else
				r = m - 1;
		}

		// if we reach here, then element was not present
		return -1;
	}
	
	public static int binarySearchInsertion(Vector<Object> arr, Object x) {
		int l = 0, r = arr.size() - 1;
		// int m = 0;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (check(x,arr.get(m))==0)
				return m;

			// If x greater, ignore left half
			if (check(x,arr.get(m)) > 0)
				l = m + 1;

			// If x is smaller, ignore right half
			else
				r = m - 1;
		}

		// if we reach here, then element was not present
		return l;
	}
	
	public static int binarysearchpage(Vector<String> pages,Object clusteringkey ) throws ClassNotFoundException, IOException {
		int l=0;
		int h=pages.size()-1;
		while(l<= h) {
			int sum=l+h;
			int middle= sum / 2;
			Page page= getPage(pages.get(middle));
			Object lc=page.records.get(0).getClustringKey();
			Object hc=page.records.get(page.records.size() - 1).getClustringKey();

			if( (check(clusteringkey,lc) >= 0) && (check(clusteringkey,hc) <= 0)) {
				return middle;
			}
			if(check(clusteringkey,hc) >= 0 
					&& (!(page.isFull()))) {
				return middle;
			}
			if(check(clusteringkey,lc) <= 0 
					&& (!(page.isFull()))) {
				return middle;
			} 
			if (check(clusteringkey,lc) <= 0 ) {
				h=middle-1;
			}
			if (check(clusteringkey,hc) >= 0 ) {
				l=middle+1;
			}
	}
		return l;
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
	
	public void updateTable (String strTableName, String strClusteringKeyValue,
            Hashtable < String, Object > htblColNameValue) throws DBAppException{
		try {
            validateUpdate(strTableName,htblColNameValue);
            Table table=null;
            String tablePath = "src/resources/"+strTableName+"/"+ strTableName + ".ser";
            FileInputStream filein= new FileInputStream(tablePath);
            ObjectInputStream in = new ObjectInputStream(filein);
            table = (Table) in.readObject();
            String cskeyname=checkName(strTableName,htblColNameValue);
            Object cskeyvalue=null;
            String type=checkType(strTableName,htblColNameValue);
            if(type.equals("java.lang.Integer")) {
            	cskeyvalue = Integer.parseInt(strClusteringKeyValue);
            }
            else if(type.equals("java.lang.String")) {
            	cskeyvalue = strClusteringKeyValue;
            }
            else if(type.equals("java.lang.Double")) {
            	cskeyvalue= Double.parseDouble(strClusteringKeyValue);
            }
            else {
            	Date date = (Date) new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
            	cskeyvalue=date;
            }
            int pageindex=binarysearchpage(table.pages,cskeyvalue);
            String path="src/resources/"+strTableName+"/"+strTableName+"_"+pageindex+".ser";
            if(pageindex==table.pages.size()) {
            //	System.out.print("3aaa");
            	throw new DBAppException("The clustring Key does not exist");
            }
            Page page = getPage(table.pages.get(pageindex));
            int recordindex=binarySearchRecords(page.getClustringKeys(),cskeyvalue);
            if(recordindex == -1) {
            //	System.out.print("3aaa");
            	throw new DBAppException("The clustring Key does not exist");
            }
            Hashtable<String,Object> oldNameValue=page.records.get(recordindex).getColNameValue();
            htblColNameValue.put(cskeyname, cskeyvalue);
            Record record=new Record(strTableName,htblColNameValue,cskeyname);
            page.records.setElementAt(record,recordindex);
            releasePage(table.pages.get(pageindex),page);
            File f=new File(tablePath); 
			f.delete();
			FileOutputStream tableOut = new FileOutputStream(tablePath);
			ObjectOutputStream outable = new ObjectOutputStream(tableOut);
			outable.writeObject(table);
			outable.close();
			tableOut.close();
			
			//index
			
			String file= "src/resources/metadata.csv";
			Vector <String> indecies = getindecies(strTableName, file);
			for(int i=0;i<indecies.size();i++) {
				//	System.out.println("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser");
					Octree t = getOctree("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser");
					Object o1 = oldNameValue.get(t.getColoumn1());
					Object o2 = oldNameValue.get(t.getColoumn2());
					Object o3 = oldNameValue.get(t.getColoumn3());
					t.remove2(o1, o2, o3, path, record.clustringKey);
					releaseOctree("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser",t);
				}
			for(int i=0;i<indecies.size();i++) {
			//	System.out.println("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser");
				Octree t = getOctree("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser");
				Object o1 = htblColNameValue.get(t.getColoumn1());
				Object o2 = htblColNameValue.get(t.getColoumn2());
				Object o3 = htblColNameValue.get(t.getColoumn3());
				t.insert(o1, o2, o3, path, record.clustringKey);
				releaseOctree("src/resources/"+strTableName+"/"+strTableName+indecies.get(i)+".ser",t);
			}
            
		}
		catch(Exception e){
			e.printStackTrace();
		}
    }
		
	public String checkType (String TableName,Hashtable < String, Object > htblColNameValue) throws IOException {
		String type=null;
		FileReader reader=new FileReader("src/resources/metadata.csv");
		BufferedReader buff=new BufferedReader(reader);
		String line=buff.readLine();
	    while(!(line==null)) {
	    	String arr[]=line.split(",");
	    	if(arr[0].equals(TableName)) {
	    		if(htblColNameValue.containsKey(arr[1])) {	
	    		}
	    		else if(arr[3].equals("TRUE")){
	    			type=arr[2];
	    			break;
	    		}
	    	}
	    	line=buff.readLine();
	    }
		return type;
	} 
	
	public String checkName (String TableName,Hashtable < String, Object > htblColNameValue) throws IOException {
		String name=null;
		FileReader reader=new FileReader("src/resources/metadata.csv");
		BufferedReader buff=new BufferedReader(reader);
		String line=buff.readLine();
	    while(!(line==null)) {
	    	String arr[]=line.split(",");
	    	if(arr[0].equals(TableName)) {
	    		if(htblColNameValue.containsKey(arr[1])) {
	    		}
	    		else if(arr[3].equals("TRUE")){
	    			name=arr[1];
	    			break;
	    		}
	    	}
	    	line=buff.readLine();
	    }
		return name;
	} 
	
	public void validateUpdate(String strTableName, Hashtable<String,Object> htblColNameValue) throws IOException, DBAppException, ParseException {
		String file= "src/resources/metadata.csv";
		FileReader reader=new FileReader(file);
		BufferedReader buff=new BufferedReader(reader);
		String line=buff.readLine();
		boolean flag1=false;
		while(!(line==null)) {
			String arr[]=line.split(",");
			if(strTableName.equals(arr[0])) {
				flag1=true;
				break;
			}
			line=buff.readLine();
		}
		if(flag1==false) {
			throw new DBAppException("Table "+strTableName+" does not exist");
		}
		for(String colname : htblColNameValue.keySet()) {
			FileReader reader2=new FileReader(file);
			BufferedReader buff2=new BufferedReader(reader2);
			String line2=buff2.readLine();
			boolean flag2=false;
	    	while(!(line2==null)) {
	    		String arr[]=line2.split(",");
	    		if(strTableName.equals(arr[0]) && colname.equals(arr[1])) {
	    			flag2=true;
	    			if(arr[2].equals("java.lang.Integer")) {
	    				if(!(htblColNameValue.get(colname) instanceof Integer)) {
	    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type Integer");
	    				}	
	    				if(((int)htblColNameValue.get(colname))<Integer.parseInt(arr[6])
	    				|| ((int)htblColNameValue.get(colname))>Integer.parseInt(arr[7])) {
	    				    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
	    				}
	    			}
	    			else if(arr[2].equals("java.lang.String")){
	    				if(!(htblColNameValue.get(colname) instanceof String)) {
	    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type String");
	    				}	
                        if(((String)htblColNameValue.get(colname)).compareTo(arr[6])<0 
                        || ((String)htblColNameValue.get(colname)).compareTo(arr[7])>0)  {
                    	    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
	    				}
	    			}
	    			else if(arr[2].equals("java.lang.Double")) {
	    				if(!(htblColNameValue.get(colname) instanceof Double)) {
	    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type Double");
	    				}	
	    				if(((Double)htblColNameValue.get(colname))<Double.parseDouble(arr[6])
			    		|| ((Double)htblColNameValue.get(colname))>Double.parseDouble(arr[7])) {
	    				    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
			   			}
	    			}
	    			else if (arr[2].equals("java.util.Date")) {
	    				if(!(htblColNameValue.get(colname) instanceof Date)) {
	    					throw new DBAppException(htblColNameValue.get(colname)+ "is not of type Date");
	    				}
	    				Date min = (Date) new SimpleDateFormat("yyyy-MM-dd").parse(arr[6]);
	    				Date max = (Date) new SimpleDateFormat("yyyy-MM-dd").parse(arr[7]);
	    				if((((Date)htblColNameValue.get(colname)).compareTo(min)<0)
					   	|| (((Date)htblColNameValue.get(colname)).compareTo(max)>0)) {
			    		    throw new DBAppException("Value "+htblColNameValue.get(colname)+" is out of bounds");
						}
	    			} 
	    			break;
	    		}
	    		line2=buff2.readLine();
	    	}
	    	if(flag2==false) {
	    		throw new DBAppException("column "+colname+" does not exist in Table");
	    	}
	    }
    }
	
	public static int binarySearchRecords(Vector<Object> arr, Object strClusteringKeyValue) {
        int last = arr.size() - 1;
        int first = 0;
        int mid = (last + first) / 2;
        Object ck = arr.get(mid);
        while (first <= last) {
            if (check(strClusteringKeyValue, ck) == 0) {
                return mid;
            } else if (check(strClusteringKeyValue, ck) < 0) {
                last = mid - 1;
            } else {
                first = mid + 1;
            }
            mid = (last + first) / 2;
            ck = arr.get(mid);
        }
        return -1;
    }

	public boolean[] validateInput(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		String type = "";
		String max = "";
		String min = "";
		Object firstKeyValue = null;
		boolean Cluster= false;
		boolean[] result = new boolean[2];
		// checking if tablename exists
		try {
			FileReader currentFile = new FileReader("src/resources/metadata.csv");
			BufferedReader br = new BufferedReader(currentFile);
			StringBuilder stringBuilder = new StringBuilder();
			String line;
			
			for (String ColName : htblColNameValue.keySet()) {
				firstKeyValue=htblColNameValue.get(ColName);
			while ((line = br.readLine()) != null) {
				String arr[] = line.split(",");
				if (arr[0].equals(strTableName)) {
					type = arr[2];
					max = arr[7];
					min = arr[6];
					if (arr[1].equals(ColName) && arr[3].equals("TRUE")) {

						if (type.equals("java.lang.Integer") && firstKeyValue instanceof Integer) {

							if ((int) firstKeyValue < Integer.parseInt(min)
									|| (int) firstKeyValue > Integer.parseInt(max)) {
								result[0] = false;
								result[1] = false;
								return result;
							} else {

								result[0] = true;
								result[1] = true;
								Cluster=true;
							}

						} else if (type.equals("java.lang.Double") && firstKeyValue instanceof Double) {

							if ((double) firstKeyValue < Double.parseDouble(min)
									|| (double) firstKeyValue > Double.parseDouble(max)) {
								result[0] = false;
								result[1] = false;
								return result;
							} else {

								result[0] = true;
								result[1] = true;
								Cluster=true;
							}
						} else if (type.equals("java.lang.String") && firstKeyValue instanceof String) {

							if (((String) firstKeyValue).compareTo(min) < 0
									|| max.compareTo((String) firstKeyValue) < 0) {
								result[0] = false;
								result[1] = false;
								return result;
							} else {
								result[1] = true;
								Cluster=true;
							}

						}

					} // validating cluster
					else if (arr[1].equals(ColName) && arr[3].equals("FALSE")) {

						if (type.equals("java.lang.Integer") && firstKeyValue instanceof Integer) {

							if ((int) firstKeyValue < Integer.parseInt(min)
									|| (int) firstKeyValue > Integer.parseInt(max)) {
								result[0] = false;
								result[1] = false;
								return result;
							} else {

								result[1] = true;
							}
						} else if (type.equals("java.lang.Double") && firstKeyValue instanceof Double) {

							if ((double) firstKeyValue < Double.parseDouble(min)
									|| (double) firstKeyValue > Double.parseDouble(max)) {
								result[0] = false;
								result[1] = false;
								return result;
							} else {
								result[1] = true;
							}
						} else if (type.equals("java.lang.String") && firstKeyValue instanceof String) {

							if (((String) firstKeyValue).compareTo(min) < 0
									|| max.compareTo((String) firstKeyValue) < 0) {
								result[0] = false;
								result[1] = false;
								return result;
							} else {
								result[1] = true;
							}

						} else if (type.equals("java.lang.Date") && firstKeyValue instanceof Date)

						{
							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							try {
								Date datemin = dateFormat.parse(min);
								Date datemax = dateFormat.parse(max);
								if (((Date) firstKeyValue).compareTo(datemin) < 0
										|| ((Date) firstKeyValue).compareTo(datemax) > 0) {
									result[0] = false;
									result[1] = false;
									return result;
								} else {
									result[1] = true;
								}
							} catch (Exception e) {

							}
						}

					}

				} // ending table found
			}}
		} catch (IOException ignored) {
		}
		result[0]=Cluster;
		return result;
	}
	public boolean CheckRecord(Record r ,Hashtable<String,Object>x)
	{
		boolean result=true;
		Hashtable <String,Object>y=r.getColNameValue();
		for (String ColName : x.keySet()) 
		{
			if(!(x.get(ColName).equals(y.get(ColName))))
			{
				result=false;
				return result;
			}
			
		}
		return result;
	}
	public Object getClusterKey(String TableName,Hashtable<String,Object>x) 
	{
		Object result=null;
		try {
			FileReader currentFile = new FileReader("src/resources/metadata.csv");
			BufferedReader br = new BufferedReader(currentFile);
			StringBuilder stringBuilder = new StringBuilder();
			String line;
			Object key;
			for (String ColName : x.keySet()) 
			{
				key=x.get(ColName);
			while ((line = br.readLine()) != null)
			{
				String arr[] = line.split(",");
				if (arr[0].equals(TableName))
				{
					if (arr[1].equals(ColName) && arr[3].equals("TRUE"))
						result=x.get(ColName);
				}
			}
			}
	}catch(Exception e)
		{
		  System.out.println("couldn't get ClusteringKey");
		}
		return result;
		}
	public Vector<String> handellingPathNameAfterDeleting(int i, Vector<String> currentPages, String tableName) {
		
		
		for (int j = i + 1; j < currentPages.size(); j++) {
			int r = j - 1;
			String s = "src/resources/" + tableName +"/"+tableName+ "_" + r + ".ser";
			currentPages.set(j, s);
			
		}
		return currentPages;
	}
		
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {

		try {
			Hashtable<String, Object> onlyOne = new Hashtable<String, Object>();
			
			
				boolean[] myResult = validateInput(strTableName, htblColNameValue);
				Boolean firstKey = myResult[0];
				Boolean Value = myResult[1];
				System.out.println(firstKey);
				System.out.println(Value);
				if (!firstKey && !Value) {
					System.out.print("tuple with "  + " and value "  
							+ "  couldn't be deleted due to  invalid data entry");
				} else {
					Table t = null;
					FileInputStream filein = new FileInputStream(
							"src/resources/" + strTableName + "/" + strTableName + ".ser");
					ObjectInputStream in = new ObjectInputStream(filein);
					t = (Table) in.readObject();
					in.close();
					filein.close();

					if (t.pages.size() == 0) {
						System.out.println("The table is already empty");
					
					}
					Vector<String> s = indexExists(strTableName, htblColNameValue);
					System.out.print(s.get(0));
					if (!(s.get(0).equals("noIndex"))) {
                        System.out.println("indexed");
						String path = "src/resources/" + strTableName + "/" + strTableName + s.get(0) + ".ser";
						Octree tree = getOctree(path);
						String o1 = tree.getColoumn1();
						String o2 = tree.getColoumn2();
						String o3 = tree.getColoumn3();
						Object k1 = htblColNameValue.get(o1);
						Object k2 = htblColNameValue.get(o2);
						Object k3 = htblColNameValue.get(o3);
						if (!tree.find(k1, k2, k3)) {
							System.out.println("these values doesn't exist");
						} else {
							String pathh = tree.get(k1, k2, k3);
							System.out.println(pathh);
							String arr[] = pathh.split(",");
							System.out.println(arr[0]);
							System.out.println(arr[1]);
							Page page11 = getPage(arr[0]);
							String type=checkType2(strTableName);
							Object o=stringType(type,arr[1]);
							int recordIndex = binarySearchRecords(page11.getClustringKeys(),o);
							System.out.print(recordIndex);
							System.out.println(k1);
							System.out.println(k2);
							System.out.println(k3);
							System.out.println(arr[0]);
							System.out.println(page11.getRecords().get(recordIndex).getClustringKey()+"tttt");
							tree.remove2(k1, k2, k3, arr[0], page11.getRecords().get(recordIndex).getClustringKey());
							releaseOctree(path, tree);
							page11.getRecords().remove(recordIndex);
							if (page11.records.size() == 0) {
								File f = new File(arr[0]);
								f.delete();
								t.pages.remove(arr[0]);
								t.setPages(handellingPathNameAfterDeleting(t.pages.indexOf(arr[0]), t.pages, strTableName));
								releaseOctree(path, tree);
							} else {
								releasePage(arr[0], page11);

							}
							// remove b2a mn al path w al table

						}
					} else {

						if (!firstKey && Value) {
							for (int i = 0; i < t.pages.size(); i++) {

								Page p = getPage(t.pages.get(i));

								for (int j = 0; j < p.getRecords().size(); j++) {
									if (CheckRecord(p.getRecords().get(j), htblColNameValue)) {
										Vector<String> check = allIndexes(strTableName,
												p.getRecords().get(j).getColNameValue());
										if (!(check.get(0).equals("noString"))) {
											for (int c = 0; c < check.size(); c++) {
												Octree tree1 = getOctree("src/resources/" + strTableName + "/"
														+ strTableName + check.get(c) + ".ser");
												Object ck = getClusterKey(strTableName,p.getRecords().get(j).getColNameValue());
												tree1.remove2(p.getRecords().get(j).getColNameValue().get(tree1.getColoumn1()),
														p.getRecords().get(j).getColNameValue().get(tree1.getColoumn2()),
														p.getRecords().get(j).getColNameValue().get(tree1.getColoumn3()), t.pages.get(i), p.getRecords().get(j).getClustringKey());
												releaseOctree(check.get(c), tree1);
											}
										}
										p.getRecords().remove(j);
										if (p.records.size() == 0) {
											File f = new File(t.pages.get(i));
											f.delete();
											t.pages.remove(i);
											p.pageNum--;
											t.setPages(handellingPathNameAfterDeleting(i, t.pages, strTableName));
										
										} else {
											releasePage(t.pages.get(i), p);
										}
									}

								}
								File f = new File("src/resources/" + strTableName + "/" + strTableName + ".ser");
								f.delete();
								FileOutputStream tableOut = new FileOutputStream(
										"src/resources/" + strTableName + "/" + strTableName + ".ser");
								ObjectOutputStream outable = new ObjectOutputStream(tableOut);
								outable.writeObject(t);
								outable.close();
								tableOut.close();
							}
						} // ending if false true

						else {
							System.out.println("binary isa");

							if (t.pages.size() == 0) {
								System.out.println("The table is already empty");
								
							}

							else {
								int i = binarysearchpage(t.pages, getClusterKey(strTableName,htblColNameValue));

								Page p = getPage(t.pages.get(i));

								Vector<Object> allCluster = p.getClustringKeys();
								int j = binarySearch(allCluster,getClusterKey(strTableName,htblColNameValue));
								System.out.println(j);
								p.getRecords().remove(j);
								System.out.println("REcord is removed");
								System.out.println(p.getRecords().get(j).getColNameValue().get("ID"));
								Vector<String> check = allIndexes(strTableName,
										p.getRecords().get(j).getColNameValue());
								System.out.println(check.get(0));
								
								if (!(check.get(0).equals("noString"))) {
									System.out.println(check.get(0));
									for (int c = 0; c < check.size(); c++) {
										Octree tree1 = getOctree("src/resources/" + strTableName + "/" + strTableName
												+ check.get(c) + ".ser");
										Object ck = getClusterKey(strTableName,
												p.getRecords().get(j).getColNameValue());
										tree1.remove2(p.getRecords().get(j).getColNameValue().get(tree1.getColoumn1()),
												p.getRecords().get(j).getColNameValue().get(tree1.getColoumn2()),
												p.getRecords().get(j).getColNameValue().get(tree1.getColoumn3()), t.pages.get(i), ck);
										System.out.println("removed");
										releaseOctree(check.get(c), tree1);
									}
								}
								if (p.records.size() == 0) {
									File f = new File(t.pages.get(i));
									f.delete();
									t.pages.remove(i);
									p.pageNum--;
									t.setPages(handellingPathNameAfterDeleting(i, t.pages, strTableName));

								} else {
									releasePage(t.pages.get(i), p);
								}
								File f = new File("src/resources/" + strTableName + "/" + strTableName + ".ser");
								f.delete();
								FileOutputStream tableOut = new FileOutputStream(
										"src/resources/" + strTableName + "/" + strTableName + ".ser");
								ObjectOutputStream outable = new ObjectOutputStream(tableOut);
								outable.writeObject(t);
								outable.close();
								tableOut.close();

							}
						}
					}

				}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Vector<String> allIndexes(String tableName, Hashtable<String, Object> s) {
		Vector<String> result = new Vector<String>();
		if (s.size() < 3) {
			result.add("noIndex");
			return result;
		} else {
			String file = "src/resources/metadata.csv";
			
			FileReader readeri;
			try {
				readeri = new FileReader(file);
				BufferedReader buffi = new BufferedReader(readeri);
			String linei=null;
				
				while ((linei = buffi.readLine())!=null) {
					String[] arr = linei.split(",");
					if (arr[0].equals(tableName) && !(arr[4].equals("null"))) {
						if (!(result.contains(arr[4]))) {
							result.add(arr[4]);
						}
					}
				}if(result.size()==0)
				{
					result.add("noIndex");
				}
				System.out.println("infinite loop");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	public String checkType2 (String TableName) throws IOException {
		FileReader reader=new FileReader("src/resources/metadata.csv");
		BufferedReader buff=new BufferedReader(reader);
		String line=buff.readLine();
	    while(!(line==null)) {
	    	String arr[]=line.split(",");
	    	if(arr[0].equals(TableName) && arr[3].equals("TRUE")) {
	    		return arr[2];
	    	}
	    	line=buff.readLine();
	    }
		return null;
	}
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
			 String[] strarrOperators) 
			throws DBAppException {
		Vector<Record> v = new Vector<Record>();
		try {
		    if(arrSQLTerms.length -1 != strarrOperators.length) {
			    throw new DBAppException("Ratio between Selection terms and Operators is invalid");
		    }
		    for(int i=0;i<strarrOperators.length;i++) {
			    if(strarrOperators[i].equals("AND") || strarrOperators[i].equals("OR") 
					|| strarrOperators[i].equals("XOR")) {
			    }
			    else
				    throw new DBAppException("Invalid operation");
		    }
		    ValidateSelect(arrSQLTerms);
		    //All Verified
		    Table table= null;
			FileInputStream filein = new FileInputStream("src/resources/"+arrSQLTerms[0]._strTableName+"/"+arrSQLTerms[0]._strTableName+".ser");
			ObjectInputStream in = new ObjectInputStream(filein);
			table = (Table) in.readObject();
			in.close();
			filein.close();
			if(table.pages.isEmpty()) {
				throw new DBAppException("Table "+arrSQLTerms[0]._strTableName+" has no records");
			}
			for(int i=0;i<arrSQLTerms.length;i++) {
				if(i==0 || strarrOperators[i-1].equals("OR")) {
					v=Addhelper(table,v,arrSQLTerms[i]);
				}
				else if(strarrOperators[i-1].equals("AND")) {
					v=ANDhelper(v,arrSQLTerms[i]);
				}
				else if(strarrOperators[i-1].equals("XOR")) { 
					Vector<Record>allothers=new Vector<Record>();
					allothers=Addhelper(table,allothers,arrSQLTerms[i]); // others+intersections
					for(int j=0;j<allothers.size();j++) {
						if(Exists(v,allothers.get(j))) {
							allothers.remove(j);//removing intersections
							j--;
						}
					}
					v=XORhelper(v,arrSQLTerms[i]); //removing intersections
					for(int j=0;j<allothers.size();j++) {
						v.add(allothers.get(j));
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		Iterator it = v.iterator();
		return it;
	}
	
	public static Vector<Record> ANDhelper(Vector<Record> v, SQLTerm term) {
		for(int i=0;i<v.size();i++) {
			Record r=v.get(i);
			if(term._strOperator.equals(">")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) <= 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals(">=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) < 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("<")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) >= 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("<=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) > 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("!=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) == 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) != 0) {
					v.remove(r);
					i--;
				}
			}
		}
		return v;
	}
	
	public static Vector<Record> Addhelper(Table table, Vector<Record> v, SQLTerm term) throws ClassNotFoundException, IOException {
		for(int j=0;j<table.pages.size();j++) { // passing each page
			Page page=getPage(table.pages.get(j));
			for(Record r: page.records) {  // passing each record in that page
				if(term._strOperator.equals(">")) {
					if(check(r.colNameValue.get(term._strColumnName),term._objValue) > 0) {
						if(!(Exists(v,r))) {
							v.add(r);
						}
					}
				}
				else if(term._strOperator.equals(">=")) {
					if(check(r.colNameValue.get(term._strColumnName),term._objValue) >= 0) {
						if(!(Exists(v,r))) {
							v.add(r);
						}
					}
				}
				else if(term._strOperator.equals("<")) {
					if(check(r.colNameValue.get(term._strColumnName),term._objValue) < 0) {
						if(!(Exists(v,r))) {
							v.add(r);
						}
					}
				}
				else if(term._strOperator.equals("<=")) {
					if(check(r.colNameValue.get(term._strColumnName),term._objValue) <= 0) {
						if(!(Exists(v,r))) {
							v.add(r);
						}
					}
				}
				else if(term._strOperator.equals("!=")) {
					if(check(r.colNameValue.get(term._strColumnName),term._objValue) != 0) {
						if(!(Exists(v,r))) {
							v.add(r);
						}
					}
				}
				else if(term._strOperator.equals("=")) {
					if(check(r.colNameValue.get(term._strColumnName),term._objValue) == 0) {
						if(!(Exists(v,r))) {
						//	System.out.println("well, yesssssssss yesssssssss");
							v.add(r);
					    }
				    }
			    }
			}
			releasePage(table.pages.get(j),page);
		}
		return v;
	}
	
	public static boolean Exists(Vector<Record> v, Record r) {
		for(Record temp : v) {
			if(check(temp.getClustringKey(),r.getClustringKey())==0) {
				return true;
			}
		}
		return false;
	}
	
	public static Vector<Record> XORhelper(Vector<Record> v, SQLTerm term) {
		for(int i=0;i<v.size();i++) {
			Record r=v.get(i);
			if(term._strOperator.equals(">")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) > 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals(">=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) >= 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("<")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) < 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("<=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) <= 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("!=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) != 0) {
					v.remove(r);
					i--;
				}
			}
			else if(term._strOperator.equals("=")) {
				if(check(r.colNameValue.get(term._strColumnName),term._objValue) == 0) {
					v.remove(r);
					i--;
				}
			}
		}
		return v;
	}
	
	
	public static void ValidateSelect(SQLTerm[] arrSQLTerms) throws DBAppException, IOException {
		String tablename=arrSQLTerms[0]._strTableName;
		for(int i=1;i<arrSQLTerms.length;i++) {
			if(!(tablename.equals(arrSQLTerms[i]._strTableName))) {
				throw new DBAppException("Table names are different in the same selection");
			}
		}
		String file= "src/resources/metadata.csv";
		FileReader reader=new FileReader(file);
		BufferedReader buff=new BufferedReader(reader);
		String line=buff.readLine();
		boolean flag1=false;
		while(!(line==null)) {
			String arr[]=line.split(",");
			if(arr[0].equals(arrSQLTerms[0]._strTableName)) {
				flag1=true;
				break;
			}
			line=buff.readLine();
		}
		if(flag1=false) {
			throw new DBAppException("Table "+arrSQLTerms[0]._strTableName+" does not exist");
		}
		
		for(int i=0;i<arrSQLTerms.length;i++) {
			FileReader reader2=new FileReader(file);
			BufferedReader buff2=new BufferedReader(reader2);
	        String line2=buff2.readLine();
			boolean flag2=false;
	    	while(!(line2==null)) {
	    		String arr[]=line2.split(",");
	    		if(arrSQLTerms[i]._strTableName.equals(arr[0]) && arrSQLTerms[i]._strColumnName.equals(arr[1])) {
	    			flag2=true;
	    			if(arr[2].equals("java.lang.Integer")) {
	    				if(!(arrSQLTerms[i]._objValue instanceof Integer)) {
	    					throw new DBAppException(arrSQLTerms[i]._objValue + " is not of type Integer");
	    				}	
	    			}
	    			else if(arr[2].equals("java.lang.String")){
	    				if(!(arrSQLTerms[i]._objValue instanceof String)) {
	    					throw new DBAppException(arrSQLTerms[i]._objValue+ " is not of type String");
	    				}	
	    			}
	    			else if(arr[2].equals("java.lang.Double")) {
	    				if(!(arrSQLTerms[i]._objValue instanceof Double)) {
	    					throw new DBAppException(arrSQLTerms[i]._objValue+ " is not of type Double");
	    				}	
	    			}
	    			else if (arr[2].equals("java.util.Date")) {
	    				if(!(arrSQLTerms[i]._objValue instanceof Date)) {
	    					throw new DBAppException(arrSQLTerms[i]._objValue+ " is not of type Date");
	    				}
	    			} 
	    			break;
	    		}
	    		line2=buff2.readLine();
	    	}
	    	if(flag2==false) {
	    		throw new DBAppException("column "+ arrSQLTerms[i]._strColumnName +" does not exist in Table");
	    	}
	    }
	}
	public static Vector<Record> help(Vector<Record> v,Record r1){
		if((v.contains(r1))) {
			System.out.print(false);
		}
		return v;
		
	}	
	
	
public static void main(String[]args) throws DBAppException, IOException, ClassNotFoundException, ParseException
{
	String y="ID";
	Hashtable <String,String> ColNameType1 = new Hashtable<>();
	ColNameType1.put("ID","java.lang.Integer");
	ColNameType1.put("Name", "java.lang.String");
	ColNameType1.put("Age", "java.lang.Integer");
	Hashtable <String,String> ColNameMax1= new Hashtable<>();
	ColNameMax1.put("ID", "50000");
	ColNameMax1.put("Name", "zzzzzzzzzzzzzzz");
	ColNameMax1.put("Age", "60");
	Hashtable <String,String> ColNameMin1= new Hashtable<>();
	ColNameMin1.put("ID", "1");
	ColNameMin1.put("Name", "A");
	ColNameMin1.put("Age", "21");
	
	Hashtable <String,Object> ColNameValue1= new Hashtable<>();
	ColNameValue1.put("ID", 1);
	ColNameValue1.put("Name", "Ahmed");
	ColNameValue1.put("Age", 30);
	Hashtable <String,Object> ColNameValue2= new Hashtable<>();
	ColNameValue2.put("Name", "Ahmed");
	ColNameValue2.put("ID", 2);
	ColNameValue2.put("Age", 40);
	Hashtable <String,Object> ColNameValue3= new Hashtable<>();
	ColNameValue3.put("ID", 3);
	ColNameValue3.put("Name", "Ahmed");
	ColNameValue3.put("Age", 50);
	Hashtable <String,Object> ColNameValue4= new Hashtable<>();
	ColNameValue4.put("ID", 4);
	ColNameValue4.put("Name", "Sherif");
	ColNameValue4.put("Age", 30);
	Hashtable <String,Object> ColNameValue5= new Hashtable<>();
	ColNameValue5.put("ID", 5);
	ColNameValue5.put("Name", "Sherif");
	ColNameValue5.put("Age", 40);
	Hashtable <String,Object> ColNameValue6= new Hashtable<>();
	ColNameValue6.put("ID", 6);
	ColNameValue6.put("Name", "Sherif");
	ColNameValue6.put("Age", 50);
	Hashtable <String,Object> ColNameValue7= new Hashtable<>();
	ColNameValue7.put("ID", 7);
	ColNameValue7.put("Name", "Hamed");
	ColNameValue7.put("Age", 30);
	Hashtable <String,Object> ColNameValue8= new Hashtable<>();
	ColNameValue8.put("ID", 8);
	ColNameValue8.put("Name", "Hamed");
	ColNameValue8.put("Age", 40);
	Hashtable <String,Object> ColNameValue9= new Hashtable<>();
	ColNameValue9.put("ID", 9);
	ColNameValue9.put("Name", "Hamed");
	ColNameValue9.put("Age", 50);
	Hashtable <String,Object> ColNameValueU= new Hashtable<>();
	ColNameValueU.put("Name", "Giroud");
	ColNameValueU.put("Age", 33);
	Hashtable <String,Object> ColNameValueU2= new Hashtable<>();
	ColNameValueU2.put("Name", "HaHa");
	ColNameValueU2.put("Age", 33);
	DBApp App=new DBApp();
	String []col=new String[3];
	col[0]="ID";
	col[1]="Name";
	col[2]="Age";
	//App.createIndex("b", col);
	Page p = getPage("src/resources/b/b_0.ser");
	System.out.print(p);
	Hashtable<String,Object>x5= new Hashtable<String,Object>();
	x5.put("ID",3 );
	App.deleteFromTable("b",x5);
	Octree tree = getOctree("src/resources/b/bIDNameAgeIndex.ser");
	tree.displayNode(tree, 2);
	System.out.print(p);
	
	//System.out.print(App.binarySearchRecords(p1.getClustringKeys(), 8));
	
}}