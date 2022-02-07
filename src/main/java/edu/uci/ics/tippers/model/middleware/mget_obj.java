package edu.uci.ics.tippers.model.middleware;

import org.apache.tomcat.jni.Time;

public class mget_obj {
    private String[] id;
    private PropInfo[] property;
    public mget_obj() {

    }
    public mget_obj(String[] id, PropInfo[] property) {
        this.id = id;
        this.property = property;
    }
    public String[] getId() {
        return this.id;
    }

    public PropInfo[] getProp() {
        return this.property;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("mget_obj{");
        sb.append("id = [");
        for(int i = 0; i < this.id.length; i++) {
            sb.append("id: ").append(this.id[i]);
            if (i != this.id.length-1) sb.append(", ");
        }
        sb.append("],\nproperty=[");
        for(int i = 0; i < this.property.length; i++) {
            sb.append("{prop: ").append(this.property[i].getProp()).append(", ");
            sb.append("info: ").append(this.property[i].getInfo()).append("}");
            if (i != this.property.length-1) sb.append(",");
            sb.append("\n");
        }
        sb.append("]\n}");
        return sb.toString();
    }
}


