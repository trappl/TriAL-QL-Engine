package data.structures;

import java.io.Serializable;

public class Tripel implements Serializable {
    private final String subject;
    private final String predicate;
    private final String object;

    public Tripel(String subject, String predicate, String object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public String getSubject() {
        return subject;
    }

    public String getPredicate() {
        return predicate;
    }

    public String getObject() {
        return object;
    }
}
