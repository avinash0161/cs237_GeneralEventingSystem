package cs237;

import java.util.Iterator;
import java.util.List;

public class AndTwoMerger extends IRuleMerger{

    public String rule1, rule2;
    public String id = "AndTwoMerger";

    AndTwoMerger(IEventRule parent, String rule1, String rule2) {
        this.parent = parent;
        this.rule1 = rule1;
        this.rule2 = rule2;
    }

    IEventRule parent;

    @Override
    String id() {
        return id;
    }

    @Override
    boolean merge(List<IRecordRule> recordRuleList) {
        boolean r_rule1 = false;
        boolean r_rule2 = false;
        for (Iterator<IRecordRule> iter = recordRuleList.iterator(); iter.hasNext(); ) {
            IRecordRule rule = iter.next();
            if (rule1.equalsIgnoreCase(rule.id())) {
                r_rule1 = true;
            } else if (rule2.equalsIgnoreCase(rule.id())) {
                r_rule2 = true;
            }
            if (r_rule1 && r_rule2) {
                return true;
            }
        }

        return r_rule1 && r_rule2;
    }

    @Override
    IEventRule getParent() {
        return this.parent;
    }
}
