package cs237;

import java.util.ArrayList;
import java.util.List;

public class ThermometerGT80Rule extends IEventRule {

    @Override
    public String ruleId() { return userName() + "-" + topicName(); }

    @Override
    public String userName() {
        return "test";
    }

    @Override
    public String topicName() {
        return "Thermometer_GT_80";
    }

    @Override
    public List<IRecordRule> recordRuleList() {

        List<IRecordRule> recordRuleList = new ArrayList<>();

        RecordRule r_thermometer_lt_80 = new RecordRule(this);
        r_thermometer_lt_80.id = "r_thermometer_lt_80";
        r_thermometer_lt_80.stream = "ThermometerObservation";

        Predicate p_gt_80 = new Predicate(r_thermometer_lt_80);
        p_gt_80.id = "p_gt_80";
        p_gt_80.attribute = "temperature";
        p_gt_80.attributeType = AttributeType.INT;
        p_gt_80.operator = Operators.GREATER_EQUAL_THAN;
        p_gt_80.valueInt = 80;

        r_thermometer_lt_80.predicateList.add(p_gt_80);
        recordRuleList.add(r_thermometer_lt_80);

        return recordRuleList;
    }

    @Override
    public IRuleMerger merger() {
        // Inner Class for merger
        class Merger extends IRuleMerger {

            Merger(IEventRule parent) {
                this.parent = parent;
            }

            IEventRule parent;

            @Override
            String id() {
                return "Thermometer80Rule_m1";
            }

            @Override
            boolean merge(List<IRecordRule> recordRuleList) {
                if (recordRuleList.size() > 0)
                    return true;
                return false;
            }

            @Override
            IEventRule getParent() {
                return this.parent;
            }
        }

        return new Merger(this);
    }

}
