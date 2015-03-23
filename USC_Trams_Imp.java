import java.sql.*;
import oracle.sql.*;
import oracle.*;


public class USC_Trams_Imp {

    private static Connection conn=null;
    private final String connection;
    private final String username;
    private final String password; 
    private static Statement stmt = null;

    hw2(String username,String password,String connection) {
        this.username = username;
        this.password = password;
        this.connection = connection;
    }
    
    void getDBConnection() {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        } catch (SQLException ex) {
            System.out.println("Please install Oracle Driver.");
            return;
        }
        try {
            conn = DriverManager.getConnection(connection, username, password);
            stmt = conn.createStatement();
        } catch (SQLException e) {
            System.out.println(e);
            return;
        }
        if (conn != null) {
            System.out.println("Connection Succeeded.");
        } else {
            System.out.println("Connection failed.");
        }
    }
    
  /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
    	
    	try
    	{
    	ResultSet rs;
        USC_Trams_Imp obj = new USC_Trams_Imp("SYSTEM", "Onyourtoes", "jdbc:oracle:thin:@localhost:1521:xe");
        obj.getDBConnection();
        if(args[0].equals("window"))						//window query.......
        {
        	if(args[1].equals("student"))
        	{
        		String query="SELECT s.STUDENT_ID FROM STUDENT s WHERE SDO_INSIDE(s.shape,SDO_GEOMETRY(2003, NULL, NULL,SDO_ELEM_INFO_ARRAY(1,1003,3),SDO_ORDINATE_ARRAY("+args[2]+","+args[3]+","+args[4]+","+args[5]+"))) = 'TRUE'";
        		rs=stmt.executeQuery(query);
        		while(rs.next())
        		{
        			System.out.println(rs.getString("STUDENT_ID"));
        		}
        		
        	}
        	else if(args[1].equals("building"))
        	{
        		String query="SELECT b.BLD_ID FROM building b WHERE SDO_INSIDE(b.shape,SDO_GEOMETRY(2003, NULL, NULL,SDO_ELEM_INFO_ARRAY(1,1003,3),SDO_ORDINATE_ARRAY("+args[2]+","+args[3]+","+args[4]+","+args[5]+"))) = 'TRUE'";
        		rs=stmt.executeQuery(query);
        		while(rs.next())
        		{
        			System.out.println(rs.getString("BLD_ID"));
        		}
        		
        	}
        	else if(args[1].equals("tramstops"))
        	{
        		String query="SELECT t.tramstop_ID FROM tramstops t WHERE SDO_INSIDE(t.shape,SDO_GEOMETRY(2003, NULL, NULL,SDO_ELEM_INFO_ARRAY(1,1003,3),SDO_ORDINATE_ARRAY("+args[2]+","+args[3]+","+args[4]+","+args[5]+"))) = 'TRUE'";
        		rs=stmt.executeQuery(query);
        		while(rs.next())
        		{
        			System.out.println(rs.getString("tramstop_ID"));
        		}
        		
        	}
        }
        else if(args[0].equals("within"))						//within query 
        {
    		String query="SELECT c.BLD_ID FROM building c WHERE SDO_WITHIN_DISTANCE(c.shape,(SELECT s.shape from student s where s.STUDENT_ID='"+args[1]+"'),'distance="+args[2]+"') ='TRUE' union SELECT t.TRAMSTOP_ID FROM tramstops t WHERE SDO_WITHIN_DISTANCE(t.shape,(SELECT s.shape from student s where s.STUDENT_ID='"+args[1]+"'),'distance="+args[2]+"') = 'TRUE'";
       		rs=stmt.executeQuery(query);
    		while(rs.next())
    		{
    			System.out.println(rs.getString(1));
    		}
        }
        else if(args[0].equals("nearest-neighbour"))					//nearest neighbour
        {
        	if(args[1].equals("building"))
        	{
        		String query="SELECT b.BLD_ID FROM building b WHERE SDO_NN(b.shape, (SELECT s.shape from building s where s.BLD_ID='"+args[2]+"'), 'sdo_batch_size=10') = 'TRUE' and bld_id!='"+args[2]+"'AND ROWNUM<="+args[3];
        		rs=stmt.executeQuery(query);
        		while(rs.next())
        		{
        			System.out.println(rs.getString("BLD_ID"));
        		}
        	}
        	else if(args[1].equals("student"))
        	{
        		String query="SELECT b.student_ID FROM student b WHERE SDO_NN(b.shape, (SELECT s.shape from student s where s.student_ID='"+args[2]+"'), 'sdo_batch_size=10') = 'TRUE' and student_id!='"+args[2]+"'AND ROWNUM<="+args[3];
        		rs=stmt.executeQuery(query);
        		while(rs.next())
        		{
        			System.out.println(rs.getString("student_ID"));
        		}
        	}
        	else if(args[1].equals("tramstops"))
        	{
        		String query="SELECT b.tramstop_ID FROM tramstops b WHERE SDO_NN(b.shape, (SELECT s.shape from tramstops s where s.tramstop_ID='"+args[2]+"'), 'sdo_batch_size=10') = 'TRUE' and tramstop_id!='"+args[2]+"' AND ROWNUM<="+args[3];
        		rs=stmt.executeQuery(query);
        		while(rs.next())
        		{
        			System.out.println(rs.getString("tramstop_ID"));
        		}
        	}
        }
        else if(args[0].equals("fixed"))							//fixed queries....
        	{
        	
        		if(Integer.parseInt(args[1])==1)
        		{
        			int i=0;
        			int x[]=new int[2];
        			int y[]=new int[2];
        			int r[]=new int[2];
        			String query1="select radius from tramstops where tramstop_id in ('t2ohe','t6ssl')";
        			String query2="SELECT t.X,t.Y FROM tramstops c,TABLE(SDO_UTIL.GETVERTICES(c.shape)) t where tramstop_id in ('t2ohe','t6ssl')";
        			ResultSet rs1=stmt.executeQuery(query1);
        			while(rs1.next())
        			{
        				r[i]=Integer.parseInt(rs1.getString(1));
        				i++;
        			}
        			i=0;
        			ResultSet rs2=stmt.executeQuery(query2);
        			while(rs2.next())
        			{
        				x[i]=Integer.parseInt(rs2.getString(1));
        				y[i]=Integer.parseInt(rs2.getString(2));
        				i++;
        			}
        			String query="select s.Student_ID  FROM Student s where SDO_RELATE(s.shape,SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,4),SDO_ordinate_array("+(x[0]+r[0])+","+y[0]+","+x[0]+","+(y[0]+r[0])+","+(x[0]+r[0])+","+(y[0]+r[0])+")),'mask=ANYINTERACT') = 'TRUE' and SDO_RELATE(s.shape,SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,4),SDO_ordinate_array("+(x[1]+r[1])+","+y[1]+","+x[1]+","+(y[1]+r[1])+","+(x[1]+r[1])+","+(y[1]+r[1])+")),'mask=ANYINTERACT') = 'TRUE' union select b.Bld_ID From Building b where SDO_RELATE(b.shape,SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,4),SDO_ordinate_array("+(x[0]+r[0])+","+y[0]+","+x[0]+","+(y[0]+r[0])+","+(x[0]+r[0])+","+(y[0]+r[0])+")),'mask=ANYINTERACT') = 'TRUE' and SDO_RELATE(b.shape,SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,4),SDO_ordinate_array("+(x[1]+r[1])+","+y[1]+","+x[1]+","+(y[1]+r[1])+","+(x[1]+r[1])+","+(y[1]+r[1])+")),'mask=ANYINTERACT') = 'TRUE'";
        			rs=stmt.executeQuery(query);
            		while(rs.next())
            		{
            			System.out.println(rs.getString(1));
            			
            		}
        		}
        		else if(Integer.parseInt(args[1])==2)
        		{
        			String query="SELECT b.tramstop_ID,s.student_id FROM tramstops b,student s WHERE SDO_NN(b.shape, s.shape, 'sdo_num_res=2')= 'TRUE'";
        			rs=stmt.executeQuery(query);
            		while(rs.next())
            		{
            			System.out.print(rs.getString("tramstop_ID")+"\t");
            			System.out.println(rs.getString("student_ID"));
            		}
        		}
        		else if(Integer.parseInt(args[1])==3)
        		{
        			String query="Select * from (SELECT t.tramstop_id,count(b.BLD_ID) as buildingsCovered FROM building b,tramstops t WHERE SDO_WITHIN_DISTANCE(b.shape,t.shape,'distance=250') = 'TRUE' group by(t.tramstop_id) order by buildingsCovered DESC) where rownum<=1";
        			rs=stmt.executeQuery(query);
            		while(rs.next())
            		{
            			System.out.print(rs.getString(1)+"\t");
            			System.out.println(rs.getString(2));
            		}
        		}
        		else if(Integer.parseInt(args[1])==4)
        		{
        			String query="select * from (SELECT s.student_id,count(b.BLD_ID) as neighbourCount FROM building b,student s WHERE SDO_NN(s.shape, b.shape, 'sdo_num_res=1')= 'TRUE' group by (s.student_id) order by neighbourCount DESC) where rownum <= 5";
        			rs=stmt.executeQuery(query);
            		while(rs.next())
            		{
            			System.out.print(rs.getString(1)+"\t");
            			System.out.println(rs.getString(2));
            		}
        		}
        		
        		else if(Integer.parseInt(args[1])==5)
        		{
        			String query="SELECT  t.X as X_Coordinate, t.Y as Y_Coordinate FROM (SELECT SDO_AGGR_MBR(b.shape) as Coordinates FROM building b WHERE b.bld_name LIKE 'SS%') table1, TABLE(SDO_UTIL.GETVERTICES(table1.Coordinates)) t";
        			rs=stmt.executeQuery(query);
        			while(rs.next())
        			{
        			System.out.print(rs.getString(1)+"\t");
        			System.out.println(rs.getString(2));
        			}
        		}
        	    
        	}
    		
    	}
        catch(SQLException e)
    	{
        	e.printStackTrace();
    	}
    	
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
}