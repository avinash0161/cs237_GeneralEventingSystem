package cs237;

import java.io.Serializable;
import java.util.List;

public abstract class IEventRule implements Serializable {

    abstract String ruleId();

    abstract String userName();

    abstract String topicName();

    abstract List<IRulePredicate> predicateList();

    abstract IRuleMerger merger();

}
