package cs237;

import java.io.Serializable;

public abstract class IRulePredicate implements Serializable {

    abstract String id();

    abstract String stream();

    abstract String attribute();

    abstract Operators operator();

    abstract AttributeType attributeType();

    abstract String valueString();

    abstract int valueInt();

    abstract double valueFloat();

    abstract IEventRule getParent();
}
