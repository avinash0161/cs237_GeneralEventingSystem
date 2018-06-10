package cs237;

import java.io.Serializable;
import java.util.List;

public abstract class IRuleMerger implements Serializable {

    abstract String id();

    abstract boolean merge(List<IRecordRule> recordRuleList);

    abstract IEventRule getParent();
}
