/**
 * 
 */
package com.src;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * @author dhavallad
 *
 */
public class sqlConn { 

	public static Connection conn = null;

	public static boolean makeConn() 
	{
		Statement stmt = null;
		//ResultSet rs = null;
		
		try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String connectionUrl = "jdbc:mysql://localhost:3306/MissingValuesDB";
			String connectionUser = "root";
			String connectionPassword = "kuku";

			conn = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
			stmt = conn.createStatement();
			ResultSet nullRs = null;
			int nullCount=0;
			nullRs = stmt.executeQuery("select * from Values_1");
			while(nullRs.next()){
				ResultSetMetaData rmsNull = (ResultSetMetaData) nullRs.getMetaData();   
				int rowID=0;;
				int col = rmsNull.getColumnCount();

				for(int i=1; i<=col;i++){
					String str = rmsNull.getColumnName(i);

					if(nullRs.getString(str)==null){
						nullCount++;
					}
				}
			}
			System.out.println("null count is"+nullCount);
			int q=0;
			while(q<nullCount)
			{
				int maxConfidence = 0;
				ConfidenceList cl = new ConfidenceList();
				tableValues nullTB = new tableValues();
				//System.out.println("while for q is started");
			q++;
			String query = "select * from Values_1";
			ResultSet rsOld = null;
			rsOld = stmt.executeQuery(query);
			//this is list of column_name and  their values which inserts data of row in which null is found
			boolean nullFound=false;
			int nullColumnNumber=0;
			while(rsOld.next()){
				ArrayList<tableValues> arr = new ArrayList<tableValues>();
				ResultSetMetaData rms = (ResultSetMetaData) rsOld.getMetaData();   
				int rowID=0;;
				int col = rms.getColumnCount();

				for(int i=1; i<=col;i++){
					String str = rms.getColumnName(i);

					if(rsOld.getString(str)==null){
					//	System.out.println("null found at column"+rms.getColumnName(i));
						nullFound = true;
						rowID = rsOld.getRow();
						rowProperty  rowP = new rowProperty();
						nullColumnNumber = i;
						int j=i;
						//null is found so storing column and values info in arraylist
						for(int k=1; k<=col; k++){            
							String str1 = rms.getColumnName(k);
							if(k!=j)
							{
								//System.out.println("Column value are"+rs.getString(str1));
								tableValues val = new tableValues();
								val.setColumnName(str1);
								val.setColumnValue(rsOld.getString(str1));
								arr.add(val);
							}
							else if(k==j){
								tableValues val = new tableValues();
								val.setColumnName(str1);
								val.setColumnValue(rsOld.getString(str1));
								arr.add(val);
							}
						}
						for(int y=0;y<arr.size();y++){
							tableValues val = arr.get(y);
							val.getColumnName();
							val.getColumnValue();
							System.out.println("Other values in row are "+val.getColumnName()+ " "+val.getColumnValue());
						}
						rowP.setRowcol(arr);

					}
				}

				//}//Prachi
				if(nullFound){
					nullFound = false;
					query = "select * from Values_1 where ";
					for(int y=0;y<arr.size();y++){
						tableValues val = arr.get(y);
						if(val.getColumnValue()==null){
							query = query + val.getColumnName()+ " IS "+val.getColumnValue() +" AND ";
						}
						else{
							query = query + val.getColumnName()+ "="  + "'"+val.getColumnValue() +"'"+" AND ";}
					}
					query=query+"1=1;";
				//	System.out.println(query);
					Statement stmt1 = null;
					stmt1 = conn.createStatement();
					ResultSet rs1 = null;
					rs1 = stmt1.executeQuery(query);
					rs1.close();
					stmt1.close();
					List<Object> list = new ArrayList<Object>();
					for(int y=0;y<arr.size();y++){
						tableValues val = arr.get(y);
						list.add(val.getColumnName());

					}
					List<Object> powerSet = new ArrayList<Object>();
			//		System.out.println("list is"+list);
					powerSet = samplePowerSet(list,nullColumnNumber);
					
					System.out.println("Power set is"+powerSet);

					for(int i=0;i<powerSet.size();i++){
						List list1 = (List)powerSet.get(i);
						query = "select";
						for(int j=0;j<list1.size();j++){
							if(j==(list1.size()-1)){
								query += " "+list1.get(j);
							}else{
								query += " "+list1.get(j)+",";}
						}
						query += " from Values_1 where ";
						String NullcoumnName = "";
						for(int y=0;y<arr.size();y++){
							tableValues val = arr.get(y);
							if(list1.contains(val.getColumnName())){
								if(val.getColumnValue()==null){
									NullcoumnName  = val.getColumnName();
									continue;
								}
								else{
									query = query + val.getColumnName()+ "="  + "'"+val.getColumnValue() +"'"+" AND ";}
							}
						}
						query=query+"1=1;";
			//			System.out.println(query);
						Statement stmt2 = null;
						stmt2 = conn.createStatement();
						ResultSet rs2 = null;
						rs2 = stmt2.executeQuery(query);

						ArrayList<tableValues> arr1 = new ArrayList<tableValues>(); //to store rs values
						while(rs2.next()){

							ResultSetMetaData rms1= (ResultSetMetaData) rs2.getMetaData();    
							int col1 = rms1.getColumnCount();

							boolean flag = true;
							for(int k=1; k<=col1;k++){ 
								if(rs2.getString(k)==null){

									flag = false;
								}
							}
							for(int k=1; k<=col1;k++){ 
								if(flag){
									tableValues tv1 = new tableValues();
									tv1.setColumnName(rms1.getColumnName(k));
									tv1.setColumnValue(rs2.getString(k));
									arr1.add(tv1);}
							}
						}
						rs2.close();
						stmt2.close();
			//			System.out.println("column name   Value");
						int l=0;
						while(l<arr1.size()){
							query = "select count(*) from Values_1 where ";
							
							for(int j=0;j<list1.size();j++){
								tableValues tv1 = arr1.get(l++);
						//		System.out.println(tv1.getColumnName()+"  "+tv1.getColumnValue());
								query += tv1.getColumnName()+ " = '"+tv1.getColumnValue()+"' and " ;
							}
							query += " 1=1;";
					//		System.out.println(query);
							Statement stmt3 = null;
							stmt3 = conn.createStatement();
							ResultSet rs3 = null;
							rs3 = stmt3.executeQuery(query);
							int numerator=0;
							while(rs3.next()){
								numerator = Integer.parseInt(rs3.getString(1));
							}
							rs3.close();
							stmt3.close();
					//		System.out.println("numerator is "+numerator);
					//		System.out.println(NullcoumnName);
							int n=0;
							query = "select count(*) from Values_1 where ";
							for(int j=0;j<list1.size();j++){
								tableValues tv1 = arr1.get(n++);
								if(!(tv1.getColumnName().equalsIgnoreCase(NullcoumnName))){
									query += tv1.getColumnName()+ " = '"+tv1.getColumnValue()+"' and " ;
								}
							}
							query += " 1=1;";
				//			System.out.println(query);
							Statement stmt4 = null;
							stmt4 = conn.createStatement();
							ResultSet rs4 = null;
							rs4 = stmt4.executeQuery(query);
							int denominator=0;
							while(rs4.next()){
								denominator = Integer.parseInt(rs4.getString(1));
							}
							denominator=denominator-1;
							rs4.close();
							stmt4.close();
					//		System.out.println("denominator is "+denominator);
							int confidence = (numerator*100)/denominator;

							if(confidence>maxConfidence){
								maxConfidence = confidence;

								cl.setConfidence(maxConfidence);
								cl.setPowerSet(list1);

								List<tableValues> tl = new ArrayList<tableValues>();
								int c=0;
								for(int j=0;j<list1.size();j++){
									tableValues tv1 = arr1.get(c++);
									tableValues maintb = new tableValues();
									maintb.setColumnName(tv1.getColumnName());
									maintb.setColumnValue(tv1.getColumnValue());
									tl.add(maintb);
								}
								cl.setTb(tl);
								cl.setRowID(rowID);
								List<tableValues> tl1 = new ArrayList<tableValues>();
								int d=0;
								for(int j=0;j<arr.size();j++){
									tableValues tv1 = arr.get(d++);
									tableValues maintb = new tableValues();
									maintb.setColumnName(tv1.getColumnName());
									maintb.setColumnValue(tv1.getColumnValue());
									tl1.add(maintb);
								}
								cl.setTb1(tl1);

							}
							System.out.println("Confidence for "+list1+ " is "+confidence);


						}

					}
				}
			}
			System.out.println("Max Confidence is"+cl.getConfidence());
			System.out.println("PowerSet is"+cl.getPowerSet());
			List<tableValues> displaytable = cl.getTb();

			for(int j=0;j<displaytable.size();j++){
				tableValues tv1 = displaytable.get(j);
				System.out.println("Column Name is"+tv1.getColumnName());
				System.out.println("Column Value is"+tv1.getColumnValue());

			}
			System.out.println("Row is "+cl.getRowID());
			Statement UpdateStmt = null;
			UpdateStmt = conn.createStatement();
			int UpdateRS = 0;

			String nullColumn = null;
			List<tableValues> tvMain = new ArrayList<tableValues>();
	//		System.out.println("Power set size is"+cl.getTb().size());
		//	System.out.println("Null row size is"+cl.getTb1().size());
			if(cl.getTb().size()<=cl.getTb1().size()){
				for(int i=0;i<cl.getTb1().size();i++){
					tableValues tv = cl.getTb1().get(i);
					boolean flagCol = false;
					for(int k=0;k<displaytable.size();k++){
						tableValues tv1 =displaytable.get(k);
						if(tv1.getColumnName().equalsIgnoreCase(tv.getColumnName())){
			//				System.out.println("in if"+tv.getColumnName());
							flagCol = true;
							if(tv.getColumnValue()==null){
								nullColumn = tv.getColumnName();
								//System.out.println("null column name is"+nullColumn);
								}
						}
					}
					if(!(flagCol)){
						tableValues mainList = new tableValues();
						mainList.setColumnName(tv.getColumnName());
				//		System.out.println(mainList.getColumnName());
						mainList.setColumnValue(tv.getColumnValue());
				//		System.out.println(mainList.getColumnValue());
						tvMain.add(mainList);
						;						}	
				}
			}
			String Updatequery = "Update Values_1 set  ";

			for(int j=0;j<displaytable.size();j++){
				tableValues tv1 = displaytable.get(j);
				if(tv1.getColumnName().equalsIgnoreCase(nullColumn)){

					Updatequery+=tv1.getColumnName()+"='"+tv1.getColumnValue()+"'";
				}
			}
			Updatequery+= " where ";
			
//			System.out.println("Tv main size is"+tvMain.size());
			if(tvMain.size()>0){
				for(int m=0;m<tvMain.size();m++){
					tableValues main = tvMain.get(m);
					if(m==(tvMain.size()-1)){
						Updatequery+=main.getColumnName()+"='"+main.getColumnValue()+"' AND ";}
					else{
						Updatequery+=main.getColumnName()+"='"+main.getColumnValue()+"' AND ";}
				}
			}
			if(tvMain.size()==1){
				Updatequery+=" AND ";
			}
//			System.out.println("after tvmain"+Updatequery);
	//		System.out.println("J  size is"++displaytable.size());
			for(int j=0;j<displaytable.size();j++){
				tableValues tv1 = displaytable.get(j);
		//			System.out.println("Tv1.column name is"+tv1.getColumnName());
		//			System.out.println("null column is"+nullColumn);
		//			System.out.println("condition is "+!(tv1.getColumnName().equalsIgnoreCase(nullColumn)));
		//			System.out.println("in for j is"+j);
					if(j==(displaytable.size()-1)){
						if(!(tv1.getColumnName().equalsIgnoreCase(nullColumn))){
							
						Updatequery+=tv1.getColumnName()+"='"+tv1.getColumnValue()+"' AND ";
				//		System.out.println("in 1st if"+Updatequery);
						}
					}else{
						if(!(tv1.getColumnName().equalsIgnoreCase(nullColumn))){
						Updatequery+=tv1.getColumnName()+"='"+tv1.getColumnValue()+"' AND ";
			//		System.out.println("in 2nd if"+Updatequery);
						}}

				}
			Updatequery+="1=1;";
			//Updatequery+=" limit"+(cl.getRowID()-1)+","+cl.getRowID();
			System.out.println(Updatequery);
			UpdateRS = UpdateStmt.executeUpdate(Updatequery);
			System.out.println("Update "+UpdateRS);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return true;

	}


	public static List samplePowerSet(List list, int nullColumnNumber){
		System.out.println("Null found at column+"+nullColumnNumber);
		int n = list.size();
		int len =  (int)Math.pow(2, n);
		int nullCol = nullColumnNumber-1;
		List ans = new ArrayList<List>();

		for(int i=0; i<len ; i++){
			String bin= Integer.toBinaryString(i); //convert to binary

			while(bin.length() < list.size()) bin = "0" + bin; //pad with 0's
		//	System.out.println("After padding binary no is "+bin+" i is "+i);
			List powerset = new ArrayList<Object>();
		//	System.out.println("List six=ze is"+list.size());
			boolean flagNullColumn = false;
			for(int j= 0;j< list.size();++j){
				int count =0;
				
				if(bin.charAt(j) == '1'){
					
					
					if(j==nullCol){
						flagNullColumn = true;
					}
					for(int k=0;k<list.size();k++){
						if(bin.charAt(k) == '1'){
							count++;
						}
						//System.out.println(k+" count isssssss "+count);
					}
					if(count>1){
						powerset.add(list.get(j));
					}

				}

	//			System.out.println("j is "+j);
	//			System.out.println(flagNullColumn);
				if(j==(list.size()-1)){
					Collections.sort(powerset);
	//				System.out.println("Pwer is "+powerset);
	//				System.out.println("size is"+powerset.size()+" flag is"+flagNullColumn);
					if(powerset.size()!=0 &&  flagNullColumn)
					{
		//				System.out.println("added powerSet"+powerset);
						ans.add(powerset);
						flagNullColumn = false;}}
			}

		}
//		System.out.println("answer is"+ans);
		return ans;  
	}


	public static void main(String args[]){

		System.out.println("Make connection is"+makeConn());

	}

}
