package planner;

import parse.data.SelectData;
import plan.Plan;
import transaction.Transaction;

public interface QueryPlanner {
    Plan createPlan(SelectData selectData);
}
