package cs237;

import java.util.ArrayList;
import java.util.List;

public class Thermometer80LTRule implements IEventRule {

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
        class Predicate implements IRulePredicate {

            IEventRule parent;

            boolean result = false;

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
            public void setResult(boolean result) {
                this.result = result;
            }

            @Override
            public boolean getResult() {
                return this.result;
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
        return null;
    }

    @Override
    public boolean isNot() {
        return false;
    }
}
