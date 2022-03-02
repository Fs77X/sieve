package edu.uci.ics.tippers.execution.MiddleWare;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.MallData;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
// import edu.uci.ics.tippers.execution.ExpResult;
// import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import java.time.*;
import java.util.*;
import edu.uci.ics.tippers.fileop.Writer;

public class ops {
    private static QueryManager queryManager;
    private int removeUserData(String device_id) {
        PolicyConstants.initialize();
        String query = "DELETE from mall_observation WHERE device_id = " + device_id;
        return queryManager.runMidDelMod(query);
    }

    public String[] getPolicyIdFromEntry(String key) {
        String query = "SELECT id from user_policy WHERE key = \'" + key + "\';";
        queryManager = new QueryManager();
        return queryManager.getPolId(query);
    }

    public int removeUserEntry(String key) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "DELETE from mall_observation WHERE id = \'" + key + "\';";
        int status = queryManager.runMidDelMod(query);
        // get polid first, then delete
        String[] polid = getPolicyIdFromEntry(key);
        query = "DELETE from user_policy WHERE key = \'" + key + "\';";
        status = queryManager.runMidDelMod(query);
        query = "DELETE from user_policy_object_condition WHERE policy_id = \'" + polid[0] + "\';";
        status = queryManager.runMidDelMod(query);
        return status;
    }

    private int removeUserOC(String[] policy_id) {
        PolicyConstants.initialize();
        StringBuilder q = new StringBuilder("DELETE from user_policy_object_condition WHERE ");
        for (int i = 0; i < policy_id.length; i++) {
            q.append("policy_id = ").append(policy_id[i]);
            if (i == policy_id.length-1) q.append(" AND ");
        }
        return queryManager.runMidDelMod(q.toString());
    }
    private int removeUserPolicy(String[] policy_id) {
        PolicyConstants.initialize();
        StringBuilder q = new StringBuilder("DELETE from user_policy WHERE ");
        for (int i = 0; i < policy_id.length; i++) {
            q.append("id = ").append(policy_id[i]);
            if (i == policy_id.length-1) q.append(" AND ");
        }
        return queryManager.runMidDelMod(q.toString());
    }
    private String[] getPolicyIdFromUsr(String device_id) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "SELECT UNIQUE policy_id from user_policy_object_condition WHERE attribute = device_id AND comp_value = " + device_id;
        return queryManager.getPolId(query);
    }
    public int deletePersonalData(String device_id) {
        PolicyConstants.initialize();
        String[] polId = getPolicyIdFromUsr(device_id);
        int status = removeUserPolicy(polId);
        status = removeUserOC(polId);
        status = removeUserData(device_id);
        return status;
    }
    public MallData[] getpersonalData(String device_id) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "SELECT * from mall_observation where device_id = " + device_id;
        return queryManager.runMiddleWareQuery(query);
    } 
    public MallData[] getpersonalEntry(String id) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "SELECT * from mall_observation where id = \'" + id +"\'";
        return queryManager.runMiddleWareQuery(query);
    } 
    public MallData[] get(String querier, String prop, String info) {
        /*
        * TODO: Get query results into a class array format and return it
        */
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePoliciesMid(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW, prop, info);
        if (bePolicies == null) {
            return null;
        }
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewrite(true, true);
        // System.out.println(guard_query_union);
        MallData[] mall = queryManager.runMiddleWareQuery(guard_query_union);
        // System.out.println("Took: " + execResultUnion.toString() + " ms");
        return mall;

    }
    public Integer delete(String querier, String prop, String info) {
        /*
        * TODO: Do what I did in get but in delete
        */
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePoliciesMid(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW, prop, info);
        if (bePolicies == null) {
            return null;
        }
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewriteMiddleWare(false, true, "DELETE");
        System.out.println(guard_query_union);
        Integer status = queryManager.runMidDelMod(guard_query_union);
        // System.out.print("Took: " + execResultUnion.getTimeTaken().toMillis() + " ms");
        return status;
    }

    public int updateEntry(String updateKey, String prop, String info) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "UPDATE mall_observation SET " + prop + "= \'" + info + "\' WHERE id = \'" + updateKey + "\';";
        return queryManager.runMidDelMod(query);
    }

    public int updateMetaEntry(String updateKey, String prop, String info) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "UPDATE user_policy SET " + prop + "= \'" + info + "\' WHERE policy_id = \'" + updateKey + "\';";
        System.out.println(query);
        return queryManager.runMidDelMod(query);
    }

    public void update(){
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePolicies("7", PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewriteMiddleWare(true, true, "UPDATE");
        System.out.println(guard_query_union);
        QueryResult execResultUnion = queryManager.runTimedQueryExp(guard_query_union, 1);
        System.out.print("Took: " + execResultUnion.getTimeTaken().toMillis() + " ms");
        return;
    }
    
}
