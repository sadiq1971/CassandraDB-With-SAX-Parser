import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
/**
 * Created by sadiq on 5/14/2017.
 */
public class Connect {
    public static  Session session;
    public static Cluster cluster;

    public  Connect(String keyspace){
         cluster = Cluster.builder().addContactPoints("127.0.0.1").build();

         cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .build();

        session = cluster.connect(keyspace);
    }


}
