/**
 * Created by sadiq on 7/8/2017.
 */
public class ProgressThread  extends Thread {

    int time;
    int tt;
    volatile boolean shutdown = false;


    public ProgressThread(int totalTime){
        tt=totalTime;
        System.out.println("======================================================================" +
                "=================================================================================");
        System.out.println("Searching......");
    }

    @Override
    public void run() {
        final String CLEARLINE_FMT = "\r30s\r";
        String pbar="                                                                                      " +
                "              ";
        String bar="";
        int remainigbar=100;

        try {
            int i=0;
            while (!shutdown) {

                if((tt-i)>0) {
                    int rate = remainigbar / (tt - i);

                    for (int k = 0; k < rate; k++) {
                        bar = bar + "=";
                        remainigbar--;
                    }
                }

                String fbar=bar+pbar.substring(bar.length());


                System.out.printf("\r%s | %d sec / %d sec(Presumptive)",fbar, i,tt);
                Thread.sleep(1000);
                i++;
                if(i>tt+5){
                    double td=tt*2.2;
                    tt=(int)td;
                    remainigbar+=3;pbar=pbar+"   ";
                }


            }

            //System.out.printf("\rTime Ellasped: %d  sec\n", i);


        } catch (InterruptedException e) {
            System.out.printf(CLEARLINE_FMT + "Interrupted!\n", "");
        }
    }

    public void shutDown(){
        shutdown=true;
    }
}
