package cs237;

import java.io.Serializable;

public abstract class IPredicate implements Serializable {

    IRecordRule parent;

    IPredicate(IRecordRule parent) {
        this.parent = parent;
    }

    IRecordRule getParent() {
        return this.parent;
    }

    abstract String id();

    abstract String attribute();

    abstract Operators operator();

    abstract AttributeType attributeType();

    abstract String valueString();

    abstract int valueInt();

    abstract double valueFloat();
}
