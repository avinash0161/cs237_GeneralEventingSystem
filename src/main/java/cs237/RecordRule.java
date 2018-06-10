package cs237;

import java.util.ArrayList;
import java.util.List;

public class RecordRule extends IRecordRule {

    public String id;
    public String stream;
    public List<IPredicate> predicateList;

    RecordRule(IEventRule parent) {
        super(parent);
        predicateList = new ArrayList<>();
    }

    @Override
    String id() {
        return id;
    }

    @Override
    String stream() {
        return stream;
    }

    @Override
    List<IPredicate> predicateList() {
        return predicateList;
    }
}
