import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by sadiq on 5/9/2017.
 */

public class GettingStarted {


    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {

        new Object("Posts");
        new Connect("demo");

        ExecutorService executor = Executors.newFixedThreadPool(1);

        //pass pathname of the xml file as Posts.xml is not in my project folder.
       // executor.execute(new ParsingThread("E:\\ProjectFilesxml\\android.stackexchange.com\\Posts.xml"));
        //executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_1.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_7.xml"));
      /*  executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_3.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_4.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_5.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_6.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_7.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_8.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_9.xml"));
        executor.execute(new ParsingThread("E:\\ProjectFilesxml\\stackoverflow.com\\Posts_10.xml"));*/

        executor.shutdown();


        //executor.execute(new ParsingThread(10000,20000));
        //executor.execute(new ParsingThread(20000,30000));
        //executor.execute(new ParsingThread(30000,40000));
        //executor.execute(new ParsingThread(40000,50000));
       // Connect.cluster.close();



        /*for(int i=20;i<100000;i++) {
            Connect.session.execute("INSERT INTO demo.sadiq (id, first_name, last_name) VALUES (" + i + ",'Sadiqur', 'Rahman')");
            System.out.println(i);
        }*/

      /*  // Use select to get the user we just entered
        ResultSet results = session.execute("SELECT * FROM demo.users WHERE lastname='Rahman'");
        for (Row row : results) {
            System.out.format("%s %s\n", row.getString("firstname"), row.getString("userid"));
        }

        // Update the same user with a new age
        session.execute("update users set age = 36 where lastname = 'Jones'");

        // Select and show the change
        results = session.execute("select * from users where lastname='Jones'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
        }

        // Delete the user from the users table
        session.execute("DELETE FROM users WHERE lastname = 'Jones'");

        // Show that the user is gone
        results = session.execute("SELECT * FROM users");
        for (Row row : results) {
            System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"),  row.getString("city"), row.getString("email"), row.getString("firstname"));
        }
        // Clean up the connection by closing it*/
        //cluster.close();

    }



}