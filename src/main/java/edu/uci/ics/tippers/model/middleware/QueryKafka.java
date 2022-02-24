package edu.uci.ics.tippers.model.middleware;

// import org.apache.tomcat.jni.Time;

public class QueryKafka {
    private String id;
    private String prop;
    private String info;
    private String query;
    public QueryKafka() {

    }
    public QueryKafka(String id, String prop, String info, String query) {
        this.id = id;
        this.prop = prop;
        this.info = info;
        this.query = query;
    }
    public String getId() {
        return this.id;
    }

    public String getProp() {
        return this.prop;
    }

    public String getInfo() {
        return this.info;
    }

    public String getQuery() {
        return this.query;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("QueryKafka{");
        sb.append("id: ").append(this.id).append(", ");
        sb.append("prop: ").append(this.prop).append(", ");
        sb.append("info: ").append(this.info).append(", ");
        sb.append("query: ").append(this.query).append("}");
        return sb.toString();
    }
}


