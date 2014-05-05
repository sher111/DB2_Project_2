package testing;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import simpledb.remote.SimpleDriver;


public class CreateTestTablesPart3 {

	final static int maxSize=1000;
	
	public static void main(String[] args) {
		Connection conn = null;
		Driver d = new SimpleDriver();
		String host = "localhost";	
		String url = "jdbc:simpledb://" + host;
		
		Random rand = null;
		Statement s = null;
		
		try {
			conn = d.connect(url, null);
			s = conn.createStatement();
			s.executeUpdate("Create table test1" +
					"( a1 int," +
					"  a2 int"+
					")");
			s.executeUpdate("Create table test2" +
					"( a1 int," +
					"  a2 int"+
					")");
			s.executeUpdate("Create table test3" +
					"( a1 int," +
					"  a2 int"+
					")");
			s.executeUpdate("Create table test4" +
					"( a1 int," +
					"  a2 int"+
					")");
			s.executeUpdate("Create table test5" +
					"( a3 int," +
					"  a4 int"+
					")");

			s.executeUpdate("create sh index idx1 on test1 (a1)");
			s.executeUpdate("create eh index idx2 on test2 (a1)");
			s.executeUpdate("create bt index idx3 on test3 (a1)");
			
			for(int i=1;i<6;i++)
			{
				if(i!=5)
				{
					rand=new Random(1);// ensure every table gets the same data
					for(int j=0;j<maxSize;j++)
					{
						s.executeUpdate("insert into test"+i+" (a1,a2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
						System.out.println(i+" "+j);
					}
				}
				else//case where i=5
				{
					for(int j=0;j<maxSize/2;j++)// insert maxsize/2 records into test5
					{
						s.executeUpdate("insert into test5 (a3,a4) values("+j+","+j+ ")");
						System.out.println(i+" "+j);
					}
				}
			}
			conn.close();

//------------------------------------------------------------------------------------------------------------
//-----------------------------------------Query 1!-----------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
            System.out.println("This is the first query");
 
            //Start the time, do the query, end the time, and check the elapsed.
            long start = System.nanoTime();
            ResultSet result = s.executeQuery("Select a1, a2 from test1 Where a1 = 20");
            long end = System.nanoTime();
            long elapsed1 = end - start; 
 
            // Print out all the results
            while (result.next()) {
                int a1 = result.getInt("a1");
                int a2 = result.getInt("a2");
                System.out.println("Result: a1 is " + a1 + " and a2 is " + a2);
            }

            System.out.println("Elapsed time in nanoseconds: " + elapsed1 + "\n\n");
 
//------------------------------------------------------------------------------------------------------------
//------------------------------------------Query 2!----------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
            System.out.println("This is the second query");
 
            //Start the time, do the query, end the time, and check the elapsed.
            start = System.nanoTime(); 
            result = s.executeQuery("Select a1, a2 from test1 Where a1 = 20");
            end = System.nanoTime();
            long elapsed2 = end - start; 
 
            // Print out all the results
            while (result.next()) {
                int a1 = result.getInt("a1");
                int a2 = result.getInt("a2");
                System.out.println("Result: a1 is " + a1 + " and a2 is " + a2);
            }

            System.out.println("Elapsed time in nanoseconds: " + elapsed2 + "\n\n");

//------------------------------------------------------------------------------------------------------------
//------------------------------------------Query 3!----------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
            System.out.println("This is the third query");
 
            //Start the time, do the query, end the time, and check the elapsed.
            start = System.nanoTime(); 
            result = s.executeQuery("Select a1, a2 from test1 Where a1 = 20");
            end = System.nanoTime();
            long elapsed3 = end - start; 
 
            // Print out all the results
            while (result.next()) {
                int a1 = result.getInt("a1");
                int a2 = result.getInt("a2");
                System.out.println("Result: a1 is " + a1 + " and a2 is " + a2);
            }

            System.out.println("Elapsed time in nanoseconds: " + elapsed3 + "\n\n");

//------------------------------------------------------------------------------------------------------------
//------------------------------------------Query 4!----------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
            System.out.println("This is the fourth query");
 
            //Start the time, do the query, end the time, and check the elapsed.
            start = System.nanoTime(); 
            result = s.executeQuery("Select a1, a2 from test1 Where a1 = 20");
            end = System.nanoTime();
            long elapsed4 = end - start; 
 
            // Print out all the results
            while (result.next()) {
                int a1 = result.getInt("a1");
                int a2 = result.getInt("a2");
                System.out.println("Result: a1 is " + a1 + " and a2 is " + a2);
            }

            System.out.println("Elapsed time in nanoseconds: " + elapsed4 + "\n\n");

//------------------------------------------------------------------------------------------------------------
//------------------------------------------Query 5!----------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
           

            /*
            System.out.println("This is the fifth query");
 
            //Start the time, do the query, end the time, and check the elapsed.
            start = System.nanoTime(); 
            result = s.executeQuery("Select a1, a2 from test1 Where a1 = 20");
            long end = System.nanoTime();
            long elapsed1 = end - start; 
 
            // Print out all the results
            while (result.next()) {
                int a1 = result.getInt("a1");
                int a2 = result.getInt("a2");
                System.out.println("Result: a1 is " + a1 + " and a2 is " + a2);
            }

            System.out.println("Elapsed time in nanoseconds: " + elapsed1 + "\n\n");
            */

//------------------------------------------------------------------------------------------------------------
//---------------------------------------Post Query Comparison------------------------------------------------
//------------------------------------------------------------------------------------------------------------

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
