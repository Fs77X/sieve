package edu.uci.ics.tippers.model.middleware;

import edu.uci.ics.tippers.model.middleware.MetaData;

public class MessageMD{
    private String msg;
    private MetaData[] md;
    public MessageMD(String msg, MetaData[] md) {
        this.msg = msg;
        this.md = md; 
    }
    
    public void setMessage(String msg) {
        this.msg = msg;
    }

    public void setMD(MetaData[] metadata) {
        this.md = metadata;
    }

    public String getMessage() {
        return msg;
    }

    public MetaData[] getMD() {
        return this.md;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Message{");
        sb.append("msg = ").append(msg).append("\n");
        sb.append(", md = [\n");
        for (int i = 0; i < this.md.length; i++) {
            // sb.append("{").append("id: ").append(md[i].getId()).append(", ");
            // sb.append("shop_name: ").append(md[i].getShopName()).append(", ");
            // sb.append("obs_date: ").append(md[i].getObsDate()).append(", ");
            // sb.append("obs_time: ").append(md[i].getObsTime()).append(", ");
            // sb.append("user_interest: ").append(md[i].getUserInterest()).append(", ");
            // sb.append("device_id: ").append(md[i].getDeviceID()).append("}");
            if (i != this.md.length - 1) {
                sb.append(",\n");
            }
        }
        sb.append("\n]");
        return sb.toString();
    }
 
    
}
