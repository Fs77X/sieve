package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

public class GuardExp {

    String id;

    String querier_type;

    String querier;

    String purpose;

    String dirty;

    String action;

    Timestamp last_updated;

    List<GuardPart> guardParts;

    public GuardExp(String id, String purpose, String action, Timestamp last_updated, List<GuardPart> guardParts) {
        this.id = id;
        this.purpose = purpose;
        this.action = action;
        this.last_updated = last_updated;
        this.guardParts = guardParts;
    }

    public GuardExp() {
        this.id = UUID.randomUUID().toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQuerier_type() {
        return querier_type;
    }

    public void setQuerier_type(String querier_type) {
        this.querier_type = querier_type;
    }

    public String getQuerier() {
        return querier;
    }

    public void setQuerier(String querier) {
        this.querier = querier;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public String getDirty() {
        return dirty;
    }

    public void setDirty(String dirty) {
        this.dirty = dirty;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Timestamp getLast_updated() {
        return last_updated;
    }

    public void setLast_updated(Timestamp last_updated) {
        this.last_updated = last_updated;
    }

    public List<GuardPart> getGuardParts() {
        return guardParts;
    }

    public void setGuardParts(List<GuardPart> guardParts) {
        this.guardParts = guardParts;
    }

    public boolean isUserGuard() {
        return this.querier_type.equalsIgnoreCase("user");
    }

    /**
     * Creates the complete guarded query string
     * SELECT * FROM PRESENCE where G1 AND (P1) OR G2 AND (P2) OR .......... GN AND (PN)
     * @return
     */
    public String createQueryWithOR(){
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        for (GuardPart gp: this.guardParts) {
            queryExp.append(delim);
            queryExp.append(gp.getGuard().print());
            queryExp.append(PolicyConstants.CONJUNCTION);
            queryExp.append("(" + gp.getGuardPartition().createQueryFromPolices() + ")");
            delim = PolicyConstants.DISJUNCTION;
        }
        return queryExp.toString();
    }

    /**
     * Creates the complete guarded query string with or without hints depending on DBMS_CHOICE value
     * @return query string
     */
    public String createQueryWithUnionMid(boolean remove_duplicate, String queryType){
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        if (PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.MYSQL_DBMS)) { //adding force index hints
            for (GuardPart gp : this.guardParts) {
                queryExp.append(delim);
                queryExp.append(PolicyConstants.SELECT_ALL)
                        .append(" force index (")
                        .append(PolicyConstants.ATTRIBUTE_INDEXES.get(gp.getGuard().getAttribute()))
                        .append(" ) Where")
                        .append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                delim = remove_duplicate? PolicyConstants.UNION: PolicyConstants.UNION_ALL;
            }
        }
        else if (PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.PGSQL_DBMS)) { //no hints added
            for (GuardPart gp: this.guardParts) {
                queryExp.append(delim);
                if (queryType.equals(("SELECT"))) {
                    queryExp.append(PolicyConstants.SELECT_ALL_WHERE)
                        .append(gp.getGuard().print());
                }
                if (queryType.equals("DELETE")) {
                    queryExp.append(PolicyConstants.SELECT_ID_WHERE)
                        .append(gp.getGuard().print());
                }
                
                queryExp.append(PolicyConstants.CONJUNCTION);
                queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                delim = remove_duplicate? PolicyConstants.UNION: PolicyConstants.UNION_ALL;
            }
        }
        else {
            throw new PolicyEngineException("Unknown DBMS");
        }
        return  queryExp.toString();
    }

    /**
     * Creates the complete guarded query string with or without hints depending on DBMS_CHOICE value
     * @return query string
     */
    public String createQueryWithUnion(boolean remove_duplicate){
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        if (PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.MYSQL_DBMS)) { //adding force index hints
            for (GuardPart gp : this.guardParts) {
                queryExp.append(delim);
                queryExp.append(PolicyConstants.SELECT_ALL)
                        .append(" force index (")
                        .append(PolicyConstants.ATTRIBUTE_INDEXES.get(gp.getGuard().getAttribute()))
                        .append(" ) Where")
                        .append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                delim = remove_duplicate? PolicyConstants.UNION: PolicyConstants.UNION_ALL;
            }
        }
        else if (PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.PGSQL_DBMS)) { //no hints added
            for (GuardPart gp: this.guardParts) {
                queryExp.append(delim);
                queryExp.append(PolicyConstants.SELECT_ALL_WHERE)
                        .append(gp.getGuard().print());
                queryExp.append(PolicyConstants.CONJUNCTION);
                queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                delim = remove_duplicate? PolicyConstants.UNION: PolicyConstants.UNION_ALL;
            }
        }
        else {
            throw new PolicyEngineException("Unknown DBMS");
        }
        return  queryExp.toString();
    }

    /**
     * (Select * from TABLE_NAME where G1 OR G2 .... or GN)
     * @return
     */
    public String createGuardOnlyQuery(){
        StringBuilder queryExp = new StringBuilder();
        queryExp.append(PolicyConstants.SELECT_ALL_WHERE);
        String delim = "";
        for (GuardPart gp: this.guardParts) {
            queryExp.append(delim).append(gp.getGuard().print());
            delim = PolicyConstants.DISJUNCTION;
        }
        return  queryExp.toString();
    }

    //TODO: Should the cost be memory read cost (current value) or io read cost?
    //TODO: io read cost would mean guard scan cost is really high
    public double estimateCostofGuardScan(){
        double gcost = 0.0;
        for (GuardPart gp: this.guardParts) {
            gcost += gp.getGuard().computeL() * PolicyConstants.getNumberOfTuples() * PolicyConstants.IO_BLOCK_READ_COST;
        }
        return gcost;
    }

    public String rewriteWithoutHint() {
        StringBuilder queryExp = new StringBuilder();
        queryExp.append("WITH polEval as (");
        String delim = "";
        for (GuardPart gp : this.guardParts) {
            queryExp.append(delim);
            queryExp.append(PolicyConstants.SELECT_ALL)
                    .append(" where")
                    .append(gp.getGuard().print())
                    .append(PolicyConstants.CONJUNCTION);
            queryExp.append(gp.getGuardPartition().createQueryFromPolices());
            delim = PolicyConstants.UNION;
        }
        queryExp.append(")");
        return queryExp.toString();
    }

    public String inlineRewrite(boolean union) {
        StringBuilder queryExp = new StringBuilder();
        queryExp.append("WITH polEval as (");
        String delim = "";
        if (union) {
            for (GuardPart gp : this.guardParts) {
                queryExp.append(delim);
                queryExp.append(PolicyConstants.SELECT_ALL)
                        .append(" force index (")
                        .append(PolicyConstants.ATTRIBUTE_INDEXES.get(gp.getGuard().getAttribute()))
                        .append(" ) Where")
                        .append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                delim = PolicyConstants.UNION;
            }
        }
        else {
            queryExp.append(PolicyConstants.SELECT_ALL_WHERE);
            for (GuardPart gp: this.guardParts) {
                queryExp.append(delim).append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                delim = PolicyConstants.DISJUNCTION;
            }
        }
        queryExp.append(")");
        return queryExp.toString();
    }

        /**
     * Sieve rewrite which only uses the guards and not the UDF.
     * Choices between using CTE versus no cte, Union versus OR
     * @param cte
     * @param union
     * @return
     */
    public String queryRewriteMiddleWare(boolean cte, boolean union, String queryType) {
        String query = null;
        if (cte) query = "WITH polEval as (";
        if (queryType.equals("DELETE")) query = "DELETE from mall_observation WHERE id IN (";
        if (queryType.equals("UPDATE")) query = "";
        // if (queryType.equals("DELETE")) {
        //     query += "DELETE from mall_observation WHERE id in (";
        // }
        if (union)
            query += createQueryWithUnionMid(true, queryType); //Change it to false to have UNION ALL
        else
            query += PolicyConstants.SELECT_ALL_WHERE + createQueryWithOR();
        if(cte && queryType.equals("SELECT")) query += ") SELECT * from polEval";
        if(queryType.equals("DELETE")) query += ")";
        return query;
    }

    /**
     * Sieve rewrite which only uses the guards and not the UDF.
     * Choices between using CTE versus no cte, Union versus OR
     * @param cte
     * @param union
     * @return
     */
    public String queryRewrite(boolean cte, boolean union) {
        String query = null;
        if (cte) query = "WITH polEval as (";
        if (union)
            query += createQueryWithUnion(true); //Change it to false to have UNION ALL
        else
            query += PolicyConstants.SELECT_ALL_WHERE + createQueryWithOR();
        if(cte) query += ") SELECT * from polEval";
        return query;
    }

    public String udfRewrite(boolean union) {
        StringBuilder queryExp = new StringBuilder();
        queryExp.append("WITH polEval as (");
        String delim = "";
        if (union) {
            for (GuardPart gp : this.guardParts) {
                queryExp.append(delim);
                queryExp.append(PolicyConstants.SELECT_ALL)
                        .append(" force index (")
                        .append(PolicyConstants.ATTRIBUTE_INDEXES.get(gp.getGuard().getAttribute()))
                        .append(" ) Where")
                        .append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                queryExp.append(" hybcheck(").append(querier).append(", \'")
                        .append(gp.getId()).append("\', ")
                        .append("user_id, location_id, start_date, " +
                                "start_time, user_profile, user_group ) = 1 ");
                delim = PolicyConstants.UNION;
            }
        } else {
            queryExp.append(PolicyConstants.SELECT_ALL_WHERE);
            for (GuardPart gp : this.guardParts) {
                queryExp.append(delim).append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                queryExp.append(" hybcheck(").append(querier).append(", \'")
                        .append(gp.getId()).append("\', ")
                        .append("user_id, location_id, start_date, " +
                                "start_time, user_profile, user_group ) = 1 ");
                delim = PolicyConstants.DISJUNCTION;
            }
        }
        queryExp.append(")");
        return queryExp.toString();
    }

    /**
     * Whether to inline policies or not
     * @param union
     * @return
     */
    public String inlineOrNot(boolean union){
        StringBuilder queryExp = new StringBuilder();
        queryExp.append("WITH polEval as (");
        String delim = "";
        if (union){
            for (GuardPart gp: this.guardParts) {
                queryExp.append(delim);
                queryExp.append(PolicyConstants.SELECT_ALL)
                        .append(" force index (")
                        .append(PolicyConstants.ATTRIBUTE_INDEXES.get(gp.getGuard().getAttribute()))
                        .append(" ) Where")
                        .append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                if(gp.estimateCostOfInline() < gp.estimateCostOfUDF())
                    queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                else
                    queryExp.append(" hybcheck(").append(querier).append(", \'")
                        .append(gp.getId()).append("\', ")
                        .append("user_id, location_id, start_date, " +
                                "start_time, user_profile, user_group ) = 1 ");
                delim = PolicyConstants.UNION;
            }
        }
        else {
            queryExp.append(PolicyConstants.SELECT_ALL_WHERE);
            for (GuardPart gp: this.guardParts) {
                queryExp.append(delim).append(gp.getGuard().print())
                        .append(PolicyConstants.CONJUNCTION);
                if(gp.estimateCostOfInline() < gp.estimateCostOfUDF())
                    queryExp.append(gp.getGuardPartition().createQueryFromPolices());
                else
                    queryExp.append(" hybcheck(").append(querier).append(", \'")
                            .append(gp.getId()).append("\', ")
                            .append("user_id, location_id, start_date, " +
                                    "start_time, user_profile, user_group ) = 1 ");
                delim = PolicyConstants.DISJUNCTION;
            }
        }
        queryExp.append(")");
        return queryExp.toString();
    }


}
