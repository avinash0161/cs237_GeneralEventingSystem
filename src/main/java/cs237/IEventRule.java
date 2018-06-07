package cs237;

import java.util.List;

public interface IEventRule {

    public String ruleId();

    public String userName();

    public String topicName();

    public List<IRulePredicate> predicateList();

    public IRuleMerger merger();

    public boolean isNot();
}
