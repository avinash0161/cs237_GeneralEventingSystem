package cs237;

public class Predicate extends IPredicate {

    public String id;
    public String attribute;
    public Operators operator;
    public AttributeType attributeType;
    public String valueString;
    public int valueInt = 0;
    public double valueFloat = 0;

    Predicate(IRecordRule parent) {
        super(parent);
    }

    @Override
    String id() {
        return id;
    }

    @Override
    String attribute() {
        return attribute;
    }

    @Override
    Operators operator() {
        return operator;
    }

    @Override
    AttributeType attributeType() {
        return attributeType;
    }

    @Override
    String valueString() {
        return valueString;
    }

    @Override
    int valueInt() {
        return valueInt;
    }

    @Override
    double valueFloat() {
        return valueFloat;
    }
}
