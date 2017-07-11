import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;

/**
 * Created by sadiq on 5/14/2017.
 */
public class ParsingThread implements Runnable {
    String path;

    ParsingThread(String path) throws IOException, SAXException, ParserConfigurationException {


        this.path=path;
        System.out.println(path);

        SAXParserFactory parserFactor = SAXParserFactory.newInstance();
        SAXParser parser = parserFactor.newSAXParser();
        //SAXHandler handler = new SAXHandler(getTotalRow(path),path);

        File file=new File(path);
        //parser.parse(ClassLoader.getSystemResourceAsStream(file.getName()), handler);
        parser.parse(file,new SAXHandler(getTotalRow(path),path));

    }

    @Override
    public void run() {

    }

 int getTotalRow(String fileName) throws IOException {
     LineNumberReader lnr = new LineNumberReader(new FileReader(new File(fileName)));
     lnr.skip(Long.MAX_VALUE);
     lnr.close();
     return lnr.getLineNumber()+1;
 }
}
