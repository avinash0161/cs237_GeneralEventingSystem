package cs237;

import java.util.List;

public interface IRuleMerger {

    String id();

    boolean merge(List<IRulePredicate> predicateList);

    IEventRule getParent();
}
