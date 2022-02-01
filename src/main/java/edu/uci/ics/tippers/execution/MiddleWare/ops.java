package edu.uci.ics.tippers.execution.MiddleWare;
import edu.uci.ics.tippers.common.PolicyConstants;
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

    public void get(String querier, String prop, String info) {
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePoliciesMid(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW, prop, info);
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewrite(true, true);
        // System.out.println(guard_query_union);
        Duration execResultUnion = queryManager.runQuery(guard_query_union);
        System.out.println("Took: " + execResultUnion.toString() + " ms");
        return;

    }
    public void delete() {
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePolicies("7", PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewriteMiddleWare(false, true, "DELETE");
        System.out.println(guard_query_union);
        QueryResult execResultUnion = queryManager.runTimedQueryExp(guard_query_union, 1);
        System.out.print("Took: " + execResultUnion.getTimeTaken().toMillis() + " ms");
        return;
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
