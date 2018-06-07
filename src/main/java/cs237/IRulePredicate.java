package cs237;

public interface IRulePredicate {

    String id();

    String stream();

    String attribute();

    Operators operator();

    AttributeType attributeType();

    String valueString();

    int valueInt();

    double valueFloat();

    void setResult(boolean result);

    boolean getResult();

    IEventRule getParent();
}
