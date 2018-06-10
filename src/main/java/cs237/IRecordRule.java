package cs237;

import java.io.Serializable;
import java.util.List;

public abstract class IRecordRule implements Serializable {

    IEventRule parent;

    IRecordRule(IEventRule parent) {
        this.parent = parent;
    }

    IEventRule getParent() {
        return this.parent;
    }

    String rid;

    abstract String id();

    abstract String stream();

    void setRecordId(String rid) {
        this.rid = rid;
    }

    String getRecordId() {
        return this.rid;
    }

    abstract List<IPredicate> predicateList();
}
