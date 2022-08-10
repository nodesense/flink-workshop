package workshop.models;

//{"registertime":1493613309385,"userid":"User_4","regionid":"Region_8","gender":"OTHER"}

public class User {
    public java.sql.Timestamp event_time; // from kafka meta data

    public Long registertime;
    public  String userid;
    public String regionid;
    public String gender;

    @Override
    public String toString() {
        return "User{" +
                "event_time=" + event_time +
                "registertime=" + registertime +
                ", userid='" + userid + '\'' +
                ", regionid='" + regionid + '\'' +
                ", gender='" + gender + '\'' +
                '}';
    }
}
