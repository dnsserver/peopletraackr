package peopletraackr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PeopleTraackrApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(PeopleTraackrApplication.class, args);
    }
    
    
    

	@Override
	public void run(String... arg0) throws Exception {
		FileInputStream fstream = new FileInputStream("coding-test-data2.txt");
	    BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
	    
	    Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
        Statement stat = conn.createStatement();
        stat.execute("drop table names if exists");
        stat.execute("create table names(id int primary key, full_name varchar(255), first_name varchar(255), last_name varchar(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO names (ID, full_name, first_name, last_name) VALUES (?,?,?,?)");
        
	    
	    Pattern p = Pattern.compile("^(\\w+),\\s+(\\w+)");
	    String strLine;
	    Matcher m;
	    String key_full;
	    String key_last;
	    String key_first;
	    HashMap<String, Boolean> list = new HashMap<>();
	    //Read File Line By Line
	    int i=0;
	    while ((strLine = br.readLine()) != null)   {
	    	//System.out.println(strLine);
	    	m = p.matcher(strLine);
	    	if(m.find()){
	    		key_last = m.group(1);
	    		key_first = m.group(2);
	    		key_full = key_last +", "+ key_first;
	    		
	    		if(!list.containsKey(key_full)){
	    			i++;
	    			prep.setLong(1,  i);
		    		prep.setString(2, key_full);
		    		prep.setString(3, key_first);
		    		prep.setString(4, key_last);
		    		prep.addBatch();
		    		
		    		if(i%20000==0){
		    			prep.executeBatch();
		    			prep.clearBatch();
		    			list.clear();
		    		}
	    		}
	    	}
	    }
	    prep.executeBatch();
		prep.clearBatch();
		list.clear();
		
	    ResultSet rs = stat.executeQuery("Select count(distinct full_name) as n from names");
	    if(rs.next()){
	    	System.out.println("Unique full name: "+rs.getLong(1));
	    }
	    rs = stat.executeQuery("Select count(distinct last_name) as n from names");
	    if(rs.next()){
	    	System.out.println("Unique last name: "+rs.getLong(1));
	    }
	    rs = stat.executeQuery("Select count(distinct first_name) as n from names");
	    if(rs.next()){
	    	System.out.println("Unique first name: "+rs.getLong(1));
	    }
	    
	    
	    rs = stat.executeQuery("Select last_name, count(last_name) as n from names group by last_name order by n desc limit 10");
	    System.out.println("Most common last names: ");
	    while(rs.next()){
	    	System.out.println(rs.getString(1)+" : "+rs.getLong(2));
	    }
	    
	    rs = stat.executeQuery("Select first_name, count(first_name) as n from names group by first_name order by n desc limit 10");
	    System.out.println("Most common first names: ");
	    while(rs.next()){
	    	System.out.println(rs.getString(1)+" : "+rs.getLong(2));
	    }
	    
	    int limit = 25;
	    System.out.println("create new tables...");
	    stat.execute("drop table names3 if exists");
	    stat.execute("create table names3 as select rownum as rowid, last_name from ( select last_name from (select min(id) as id, last_name from names group by last_name) order by id asc limit "+limit+") ");
	    
	    stat.execute("drop table names4 if exists");
	    stat.execute("create table names4 as select rownum as rowid, first_name from ( select first_name from (select min(id) as id, first_name from names group by first_name) order by id desc limit "+limit+") ");

	    System.out.println("Rows deleted...selecting rows...");
	    rs = stat.executeQuery("select t1.last_name, t2.first_name from names3 t1 join names4 t2 on t1.rowid = t2.rowid;");
	    
	    System.out.println("List of modified names: ");
	    while(rs.next()){
	    	System.out.println(rs.getString(1)+", "+rs.getString(2));
	    }
	    
	    
	    //Close the input stream
	    br.close();
	    stat.close();
	    conn.close();
	}

}
