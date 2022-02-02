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

    public MallData[] getpersonalData(String device_id) {
        return null;
    } 
    public MallData[] get(String querier, String prop, String info) {
        /*
        * TODO: Get query results into a class array format and return it
        */
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

    public void update(){
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
