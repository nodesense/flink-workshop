package workshop.models;

// {"viewtime":1660137705976,"userid":"User_8","pageid":"Page_52"}

public class PageView {
    public java.sql.Timestamp event_time; // from kafka meta data
public Long viewtime;
public  String userid;
public String pageid;

    @Override
    public String toString() {
        return "PageView{" +
                "event_time=" + event_time +
                "viewtime=" + viewtime +
                ", userid='" + userid + '\'' +
                ", pageid='" + pageid + '\'' +
                '}';
    }
}
