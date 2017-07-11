import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.querybuilder.Select;

import java.util.Scanner;
import java.util.Vector;

/**
 * Created by sadiq on 7/7/2017.
 */

public class Query {
    public static Session session;
    public static Cluster cluster;


    public  Query(String keyspace){
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .build();



        cluster.getConfiguration().getQueryOptions().setDefaultIdempotence(true);
        session = cluster.connect(keyspace);

    }

    static void _max(String filed)
    {
       // System.out.println("Searching MaxScore... (we are not so fast as google)");

        Scanner sc=new Scanner(System.in);

        System.out.println("Searching in whole database will take several hours.\n" +
                "But you can search to a limited range for faster result.\n");

        int sIndex=0,eIndex=0;
        System.out.print("Starting from row: ");
        sIndex = sc.nextInt();
        System.out.print("Ending row: ");
        eIndex = sc.nextInt();

        int fsaddress=sIndex;

        double r=eIndex-sIndex;


        double fTime= 24474570 + (0.5475175 - 24474570)/(1 + Math.pow((r/1794285000),1.205143));
        if(fTime<0){
            fTime=1;
        }


       /* String query = "select max("+filed+") from demo.posts where id ";


        String range="in(\'"+String.valueOf(sIndex)+"\'";

        while(sIndex<=eIndex) {
            sIndex++;
            range=range+",\'"+String.valueOf(sIndex)+"\'";


        }
        range=range+");";
        query=query+range;
        */


        ProgressThread thread= new  ProgressThread((int)fTime+2);
        new Thread(thread).start();

        /*String _maxScore="";

        for(Row row:session.execute(new SimpleStatement(query).setReadTimeoutMillis(650000))){
           // System.out.println(row.toString());
             _maxScore=row.toString();
        }



        String maxScore = _maxScore.substring(4,_maxScore.length()-1);*/

        int mS=1;

       /* switch (filed){
            case "score":{ mS=Integer.valueOf(maxScore)-10000;if(mS<0){mS=1;}break;}
            case "answercount":{ mS=Integer.valueOf(maxScore)-350;if(mS<0){mS=1;}break;}
            case "viewcount":{ mS=Integer.valueOf(maxScore)-3000000;if(mS<0){mS=1;}}

        }*/

        //System.out.println(mS);


        String finalQuery="select id,owneruserid,"+filed+",title from demo.posts where id ";

        String finalrange ="in(\'"+String.valueOf(fsaddress)+"\'";

        while(fsaddress<=eIndex) {
            fsaddress++;
            finalrange=finalrange+",\'"+String.valueOf(fsaddress)+"\'";
        }


        finalrange=finalrange+") and "+filed+"> 1 allow filtering;";
        finalQuery=finalQuery+finalrange;



        /*String finalQuery="select id,owneruserid,score,title from demo.posts where score > "+mS +"" +
                " allow filtering;";*/

       // System.out.println(finalQuery);


        Vector<java.lang.String> results=new Vector<>();
        Vector<Integer> scores=new Vector<>();

        for(Row row:session.execute(new SimpleStatement(finalQuery).setReadTimeoutMillis(650000))){

            results.add(row.toString());
        }


        thread.shutDown();
        System.out.println();
        System.out.println("Result:");
        //System.out.println("Max"+filed+" Found: "+maxScore);


        System.out.println("postid | ownerid | "+filed+" | title");

        int size=0;

        for(String s:results){
           // System.out.println(s);
            String[] ss=s.split(",");
            scores.add(Integer.valueOf(ss[2].trim()));
            size++;
        }

        for(int i=0;i<size-1;i++){
            for(int j=0;j<size-i-1;j++){
                if(scores.get(j)<scores.get(j+1)){
                    String sString=results.get(j);
                    int temp=scores.get(j);
                    scores.set(j,scores.get(j+1));
                    scores.set(j+1,temp);
                    results.set(j,results.get(j+1));
                    results.set(j+1,sString);
                }
            }
        }

        for(int i=0;i<10;i++){
            System.out.println(results.get(i));
           // System.out.println(scores.get(i));

        }

        System.out.println("======================================================================" +
                "=================================================================================");

    }




    static void TotalRow(){

       ProgressThread thread= new  ProgressThread(3600);
       new Thread(thread).start();


        //System.out.println("Counting rows..");

        String query="select count(id) from demo.posts;";
        for(Row row:session.execute(new SimpleStatement(query).setReadTimeoutMillis(650000))){
            thread.shutDown();

            System.out.println();
            System.out.println("Result:");
            System.out.println(row.toString());
        }

        System.out.println("======================================================================" +
                "=================================================================================");

    }

    static void TopTag(){


        Scanner sc=new Scanner(System.in);

        System.out.println("Searching in whole database will take several hours.\n" +
                "But you can search to a limited range for faster result.\n");
        int sIndex=0,eIndex=0;


                System.out.print("Starting from row: ");
                sIndex = sc.nextInt();
                System.out.print("Ending row: ");
                eIndex = sc.nextInt();

                double r=eIndex-sIndex;


                double fTime=.00000054978 *Math.pow(r,2) + .00331476934 *r + 1.525137594;
                if(fTime<0){
                    fTime=1;
                }



                ProgressThread thread = new ProgressThread((int)fTime);
                new Thread(thread).start();


                String query = "select mapmax(group_and_count(tag)) from demo.tags where id ";


                String range="in(\'"+String.valueOf(sIndex)+"\'";

                while(sIndex<=eIndex) {
                    sIndex++;
                    range=range+",\'"+String.valueOf(sIndex)+"\'";


                }
                range=range+");";
                query=query+range;



               // System.out.println(query);
                for (Row row : session.execute(new SimpleStatement(query).setReadTimeoutMillis(650000))) {
                    thread.shutDown();
                    System.out.println();
                    System.out.println("Result:");
                    System.out.println(row.toString());
                }

                System.out.println("======================================================================" +
                        "=================================================================================");

    }

    static void PercentageOfRegUser(){


        Scanner sc=new Scanner(System.in);

        System.out.println("Searching in whole database will take several hours.\n" +
                "But you can search to a limited range for faster result.\n");
        int sIndex=0,eIndex=0;

        System.out.print("Starting from row: ");
        sIndex = sc.nextInt();
        System.out.print("Ending row: ");
        eIndex = sc.nextInt();

        double r=eIndex-sIndex;


        double fTime=.00000054978 *Math.pow(r,2) + .00331476934 *r + 1.525137594;
        if(fTime<0){
            fTime=1;
        }

        ProgressThread thread= new  ProgressThread((int)fTime);
        new Thread(thread).start();


        String LastDate="$$2011-02-06 03:59:44+0000$$";

        String date1="$$2011-01-01 00:00:00+0000$$";
        String date2="$$2010-01-01 00:00:00+0000$$";
        String date3="$$2009-01-01 00:00:00+0000$$";
        String date4="$$2008-01-01 00:00:00+0000$$";

        Vector<String>query=new Vector<>();
        Vector<String >year=new Vector<>();
        year.add("2011");
        year.add("2010");
        year.add("2009");
        year.add("2008");


        String range="in(\'"+String.valueOf(sIndex)+"\'";

        while(sIndex<=eIndex) {
            sIndex++;
            range=range+",\'"+String.valueOf(sIndex)+"\'";


        }
        range=range+") and ";




        query.add("select count(AcceptedAnsId),count(id) from demo.posts where id "+range +
                "creationdate>"+date1 + " allow filtering;");
        query.add("select count(AcceptedAnsId),count(id) from demo.posts where id " +range+
                "creationdate>"+date2 + " and creationdate<"+ date1+" allow filtering;");
        query.add("select count(AcceptedAnsId),count(id) from demo.posts where id " +range+
                "creationdate>"+date3 + " and creationdate<"+ date2+" allow filtering;");
        query.add("select count(AcceptedAnsId),count(id) from demo.posts where id " +range+
                "creationdate>"+date4 + " and creationdate<"+ date3+" allow filtering;");

        int i=0;
        for(String q:query){
            for(Row row:session.execute(new SimpleStatement(q).setReadTimeoutMillis(650000))){
                System.out.println();
               // System.out.println("Result:");
                //System.out.println(row.toString());

                String res[]=row.toString().split(",");
                int regUser=Integer.valueOf(res[0].substring(4));
                int totalUser=Integer.valueOf(res[1].trim().substring(0,res[1].length()-2));

                if(totalUser>0) {
                    System.out.printf("Year: %s | Total user:%d  | Reg.User:%d  | Percentage:%d%%"
                            , year.get(i), totalUser, regUser, (regUser * 100) / totalUser);
                    System.out.println();
                }
                else {
                    System.out.printf("Year: %s | Total user:%d  | Reg.User:%d  | Percentage:%d%%"
                            , year.get(i), totalUser, regUser, 0);
                    System.out.println();

                }
            }
            i++;
        }
        thread.shutDown();



        System.out.println("======================================================================" +
                "=================================================================================");


    }


    public static void main(String[] args) throws InterruptedException {

        new Query("demo");
        boolean isEnd=false;

        while (!isEnd){
            System.out.println();
            System.out.println("Press 1 for Most Popular Posts");
            System.out.println("Press 2 for Most Viewed Posts");
            System.out.println("Press 3 for Most Responded Posts ");
            System.out.println("Press 4 for Top Category");
            System.out.println("Press 5 for Percentage of Registered User in different year" );
            System.out.println("Press 0 for total number of row");

            Scanner sc=new Scanner(System.in);

            switch (sc.nextInt()){
                case 0:{TotalRow();break;}
                case 1:{_max("score");break;}
                case 2:{_max("viewcount");break;}
                case 3:{_max("answercount");break;}
                case 4:{TopTag();break;}
                case 5:{PercentageOfRegUser();break;}
                case 10:{cluster.close();isEnd=true;}
            }

        }
    }
}
