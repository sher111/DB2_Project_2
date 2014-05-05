package testing;

import java.sql.Connection;

import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import simpledb.remote.SimpleDriver;

public class CreateTestTablesPart5 {

    final static int maxSize = 10;

    public static void main(String[] args) {
        Connection conn=null;
        Driver d = new SimpleDriver();
        String host = "localhost";
        String url = "jdbc:simpledb://" + host;
        Random rand=null;
        Statement s=null;

        final int seed = 1; // This is a seed used for consistent testing.
 
        try {
            conn = d.connect(url, null);
            s=conn.createStatement();
  
            // Creates table test1 with a1 and a2
            s.executeUpdate("Create table test1 " + "(a1 int," + "  a2 int" + ")"); 
  
            // Creates table test2 with a3 and a4
            s.executeUpdate("Create table test2 " + "(a3 int," + "  a4 int" + ")");
   
            // Here, we are populating the tables with tuples containing randomly-generated numbers
            rand=new Random(seed);// use a seed for consistent testing!
            for(int j=0;j<maxSize;j++) {
                s.executeUpdate("insert into test1 (a1, a2) values(" + rand.nextInt(1000) + "," + rand.nextInt(1000)+ ")");
            }
    
            for(int j=0;j<maxSize;j++) {
                s.executeUpdate("insert into test2 (a3, a4) values(" + rand.nextInt(1000) + "," + rand.nextInt(1000)+ ")");
            }

//------------------------------------------------------------------------------------------------------------
//-----------------------------------------Query 1!-----------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
            System.out.println("This is the first query");
 
            //Start the time, do the query, end the time, and check the elapsed.
            long start = System.nanoTime();
            ResultSet result = s.executeQuery("select a1, a3 from test1, test2 where a1 = a3");
            long end = System.nanoTime();
            long elapsed1 = end - start; 
 
            // Print out all the results
            while (result.next()) {
                int a1 = result.getInt("a1");
                int a3 = result.getInt("a3");
                System.out.println("Result: a1 is " + a1 + " and a3 is " + a3);
            }

            System.out.println("Elapsed time in nanoseconds: " + elapsed1 + "\n\n");
 
//------------------------------------------------------------------------------------------------------------
//------------------------------------------Query 2!----------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
            System.out.println("This is the second query");
 
            //Start the time, do the query, end the time, and check the elapsed.
            start = System.nanoTime(); 
            result = s.executeQuery("select a1, a3 from test1, test2 where a1 = a3");
            end = System.nanoTime(); 
            long elapsed2 = end - start;
 
            // Print out all the results
            while (result.next()) {
                int a1 = result.getInt("a1");
                int a3 = result.getInt("a3");
                System.out.println("Result: a1 is " + a1 + " and a3 is " + a3);
            }

            System.out.println("Elapsed time in nanoseconds: " + elapsed2 + "\n\n");

//------------------------------------------------------------------------------------------------------------
//---------------------------------------Post Query Comparison------------------------------------------------
//------------------------------------------------------------------------------------------------------------

            float ratio = (elapsed1 / elapsed2);
 
            System.out.println("Speed Test! First query took " + elapsed1 + "nanoseconds and second query took " + elapsed2 + " nanoseconds.");
            System.out.println("The second query is " + ratio + " times faster than the first. As you can see, the second query is much faster.\n");

            //Done query testing. :D
            conn.close();
 
            } catch (SQLException e) {
                e.printStackTrace();
            }finally{
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
