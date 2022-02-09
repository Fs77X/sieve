package edu.uci.ics.tippers.model.middleware;

// import org.apache.tomcat.jni.Time;

public class mget_obj {
    private String id;
    private String prop;
    private String info;
    public mget_obj() {

    }
    public mget_obj(String id, String prop, String info) {
        this.id = id;
        this.prop = prop;
        this.info = info;
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

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("mget_obj{");
        sb.append("id: ").append(this.id).append(", ");
        sb.append("prop: ").append(this.prop).append(", ");
        sb.append("info: ").append(this.info).append("}");
        return sb.toString();
    }
}


