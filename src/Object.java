import java.util.Vector;

/**
 * Created by sadiq on 5/14/2017.
 */
public class Object {
    public static Vector<String> list;
    Object(String name)
    {
        if(name=="Posts") {
            list = new Vector<>();
            list.add("Id");//0
            list.add("PostTypeId");//1
            list.add("AcceptedAnswerId");//2
            list.add("CreationDate");//3
            list.add("Score");//4
            list.add("ViewCount");//5
            list.add("Body");//6
            list.add("OwnerUserId");//7
            list.add("Title");//8
            list.add("Tags");// 9 number
            list.add("AnswerCount");//10
            list.add("CommentCount");//11
        }


    }
}
