package workshop.models;

// join result
public class PageViewUser {
    public  String userid;

    public Long viewtime;
    public String pageid;

    public String regionid;
    public String gender;

    @Override
    public String toString() {
        return "PageViewUser{" +
                "userid='" + userid + '\'' +
                ", viewtime=" + viewtime +
                ", pageid='" + pageid + '\'' +
                ", regionid='" + regionid + '\'' +
                ", gender='" + gender + '\'' +
                '}';
    }
}
