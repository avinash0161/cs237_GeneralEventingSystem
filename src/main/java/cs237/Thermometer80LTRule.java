package cs237;

import java.util.ArrayList;
import java.util.List;

public class Thermometer80LTRule extends IEventRule {

    @Override
    public String ruleId() { return userName() + "-" + topicName(); }

    @Override
    public String userName() {
        return "test";
    }

    @Override
    public String topicName() {
        return "Thermometer_LT_80";
    }

    @Override
    public List<IRulePredicate> predicateList() {
        // Inner Class for predicate
        class Predicate extends IRulePredicate {

            IEventRule parent;

            Predicate(IEventRule parent) {
                this.parent = parent;
            }

            @Override
            public String id() {
                return "Thermometer80Rule_p1";
            }

            @Override
            public String stream() {
                return "ThermometerObservation";
            }

            @Override
            public String attribute() {
                return "temperature";
            }

            @Override
            public Operators operator() {
                return Operators.LARGER_EQUAL_THAN;
            }

            @Override
            public AttributeType attributeType() {
                return AttributeType.INT;
            }

            @Override
            public String valueString() {
                return null;
            }

            @Override
            public int valueInt() {
                return 80;
            }

            @Override
            public double valueFloat() {
                return 0;
            }

            @Override
            public IEventRule getParent() {
                return parent;
            }
        }

        List<IRulePredicate> rulePredicateList = new ArrayList<>();
        rulePredicateList.add(new Predicate(this));
        return rulePredicateList;
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
            boolean merge(List<IRulePredicate> predicateList) {
                return true;
            }

            @Override
            IEventRule getParent() {
                return null;
            }
        }

        return new Merger(this);
    }

}
