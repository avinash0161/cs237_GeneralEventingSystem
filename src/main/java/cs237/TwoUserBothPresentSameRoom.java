package cs237;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TwoUserBothPresentSameRoom extends IEventRule {
    @Override
    String ruleId() {
        return userName() + "-" + topicName();
    }

    @Override
    String userName() {
        return "test";
    }

    @Override
    String topicName() {
        return "TwoUserBothPresentSameRoom";
    }

    @Override
    List<IRecordRule> recordRuleList() {

        List<IRecordRule> recordRuleList = new ArrayList<>();

        RecordRule r_user8 = new RecordRule(this);
        r_user8.id = "r_user8";
        r_user8.stream = "PRESENCE";

        Predicate p_user8 =  new Predicate(r_user8);
        p_user8.id = "p_user8";
        p_user8.attribute = "semantic_entity_id";
        p_user8.attributeType = AttributeType.STRING;
        p_user8.operator = Operators.EQUAL;
        p_user8.valueString = "user8";


        Predicate p_room_user8 =  new Predicate(r_user8);
        p_room_user8.id = "p_room_user8";
        p_room_user8.attribute = "location";
        p_room_user8.attributeType = AttributeType.STRING;
        p_room_user8.operator = Operators.EQUAL;
        p_room_user8.valueString = "2032";

        r_user8.predicateList.add(p_user8);
        r_user8.predicateList.add(p_room_user8);

        recordRuleList.add(r_user8);

        RecordRule r_user6 = new RecordRule(this);
        r_user6.id = "r_user6";
        r_user6.stream = "PRESENCE";

        Predicate p_user6 =  new Predicate(r_user6);
        p_user6.id = "p_user6";
        p_user6.attribute = "semantic_entity_id";
        p_user6.attributeType = AttributeType.STRING;
        p_user6.operator = Operators.EQUAL;
        p_user6.valueString = "user6";

        Predicate p_room_user6 =  new Predicate(r_user6);
        p_room_user6.id = "p_room_user6";
        p_room_user6.attribute = "location";
        p_room_user6.attributeType = AttributeType.STRING;
        p_room_user6.operator = Operators.EQUAL;
        p_room_user6.valueString = "2032";

        r_user6.predicateList.add(p_user6);
        r_user6.predicateList.add(p_room_user6);

        recordRuleList.add(r_user6);

        return recordRuleList;
    }

    @Override
    IRuleMerger merger() {

        // Inner Class for merger
        class Merger extends IRuleMerger {

            Merger(IEventRule parent) {
                this.parent = parent;
            }

            IEventRule parent;

            @Override
            String id() {
                return "TwoUserBothPresent2ndFloor_m1";
            }

            @Override
            boolean merge(List<IRecordRule> recordRuleList) {
                boolean r_user8 = false;
                boolean r_user6 = false;
                for (Iterator<IRecordRule> iter = recordRuleList.iterator(); iter.hasNext();) {
                    IRecordRule rule = iter.next();
                    if (rule.id() == "r_user8") {
                        r_user8 = true;
                    }
                    else if (rule.id() == "r_user6") {
                        r_user6 = true;
                    }
                    if (r_user8 && r_user6) {
                        return true;
                    }
                }

                return r_user8 && r_user6;
            }

            @Override
            IEventRule getParent() {
                return this.parent;
            }
        }

        return null;
    }
}
