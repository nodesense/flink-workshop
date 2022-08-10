package workshop.models;

//{"registertime":1493613309385,"userid":"User_4","regionid":"Region_8","gender":"OTHER"}

public class User {
    public Long registertime;
    public  String userid;
    public String regionid;
    public String gender;

    @Override
    public String toString() {
        return "User{" +
                "registertime=" + registertime +
                ", userid='" + userid + '\'' +
                ", regionid='" + regionid + '\'' +
                ", gender='" + gender + '\'' +
                '}';
    }
}
