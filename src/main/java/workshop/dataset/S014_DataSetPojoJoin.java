package workshop.dataset;


import org.apache.flink.api.java.DataSet;
        import org.apache.flink.api.java.ExecutionEnvironment;
        import org.apache.flink.api.java.tuple.Tuple2;

        import java.util.List;

public class S014_DataSetPojoJoin {

    /** POJO class representing a user. */
    public static class User {
        public int userIdentifier;
        public String name;

        public User() {}

        public User(int userIdentifier, String name) {
            this.userIdentifier = userIdentifier;
            this.name = name;
        }

        public String toString() {
            return "User{userIdentifier=" + userIdentifier + " name=" + name + "}";
        }
    }

    /** POJO for an EMail. */
    public static class EMail {
        public int userId;
        public String subject;
        public String body;

        public EMail() {}

        public EMail(int userId, String subject, String body) {
            this.userId = userId;
            this.subject = subject;
            this.body = body;
        }

        public String toString() {
            return "eMail{userId=" + userId + " subject=" + subject + " body=" + body + "}";
        }
    }

    public static void main(String[] args) throws Exception {
        // initialize a new Collection-based execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        // create objects for users and emails
        User[] usersArray = {new User(1, "Peter"), new User(2, "John"), new User(3, "Bill")};

        EMail[] emailsArray = {
                new EMail(1, "Re: Meeting", "How about 1pm?"),
                new EMail(1, "Re: Meeting", "Sorry, I'm not available"),
                new EMail(3, "Re: Re: Project proposal", "Give me a few more days to think about it.")
        };

        // convert objects into a DataSet
        DataSet<User> users = env.fromElements(usersArray);
        DataSet<EMail> emails = env.fromElements(emailsArray);

        // join the two DataSets
        DataSet<Tuple2<User, EMail>> joined =
                users.join(emails).where("userIdentifier").equalTo("userId");

        // retrieve the resulting Tuple2 elements into a ArrayList.
        List<Tuple2<User, EMail>> result = joined.collect();

        System.out.println("Join result count " + result.size());

        // Do some work with the resulting ArrayList (=Collection).
        for (Tuple2<User, EMail> t : result) {
            System.err.println("Result = " + t);
        }
    }
}