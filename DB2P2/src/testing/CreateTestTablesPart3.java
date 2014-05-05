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
		String host = "localhost";			//you may change it if your SimpleDB server is running on a different machine
		String url = "jdbc:simpledb://" + host;
		/*String qry="Create table test1" +
				"( a1 int," +
				"  a2 int"+
				")";*/
		
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
					"( a1 int," +
					"  a2 int"+
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
					for(int j=0;j<maxSize/2;j++)// insert 10000 records into test5
					{
						s.executeUpdate("insert into test"+i+" (a1,a2) values("+j+","+j+ ")");
						System.out.println(i+" "+j);
					}
				}
			}
			conn.close();

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

//s.executeUpdate("Select [ALL Attributes] from test" + i + " Where a" + (2*i) + " = 20");