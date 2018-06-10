package cs237;

import java.util.ArrayList;
import java.util.List;

public class TwoUserPresentSameFloor extends IEventRule {

    String user1, user2, floor;

    TwoUserPresentSameFloor(String user1, String user2, String floor) {
        this.user1 = user1;
        this.user2 = user2;
        this.floor = floor;
    }

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
        return "TwoUserPresentSameFloor";
    }

    @Override
    List<IRecordRule> recordRuleList() {

        List<IRecordRule> recordRuleList = new ArrayList<>();

        RecordRule r_user1_on_floor = new RecordRule(this);
        r_user1_on_floor.id = "r_user1_on_floor";
        r_user1_on_floor.stream = "PRESENCE";

        Predicate p_user1 = new Predicate(r_user1_on_floor);
        p_user1.id = "p_user1";
        p_user1.attribute = "semantic_entity_id";
        p_user1.attributeType = AttributeType.STRING;
        p_user1.operator = Operators.EQUAL;
        p_user1.valueString = this.user1;

//        Predicate p_user8_above_floor = new Predicate(r_user8);
//        p_user8_above_floor.id = "p_user8_above_floor";
//        p_user8_above_floor.attribute = "location";
//        p_user8_above_floor.attributeType = AttributeType.STRING;
//        p_user8_above_floor.operator = Operators.GREATER_THAN;
//        p_user8_above_floor.valueString = "1000";
//
//        Predicate p_user8_below_floor = new Predicate(r_user8);
//        p_user8_below_floor.id = "p_user8_below_floor";
//        p_user8_below_floor.attribute = "location";
//        p_user8_below_floor.attributeType = AttributeType.STRING;
//        p_user8_below_floor.operator = Operators.LESS_THAN;
//        p_user8_below_floor.valueString = "3000";

        Predicate p_user1_floor = new Predicate(r_user1_on_floor);
        p_user1_floor.id = "p_user1_floor";
        p_user1_floor.attribute = "location";
        p_user1_floor.attributeType = AttributeType.STRING;
        p_user1_floor.operator = Operators.BEGIN_WITH;
        p_user1_floor.valueString = this.floor;

        r_user1_on_floor.predicateList.add(p_user1);
        r_user1_on_floor.predicateList.add(p_user1_floor);

        recordRuleList.add(r_user1_on_floor);

        RecordRule r_user2_on_floor = new RecordRule(this);
        r_user2_on_floor.id = "r_user2_on_floor";
        r_user2_on_floor.stream = "PRESENCE";

        Predicate p_user2 = new Predicate(r_user2_on_floor);
        p_user2.id = "p_user2";
        p_user2.attribute = "semantic_entity_id";
        p_user2.attributeType = AttributeType.STRING;
        p_user2.operator = Operators.EQUAL;
        p_user2.valueString = this.user2;

        Predicate p_user2_floor = new Predicate(r_user2_on_floor);
        p_user2_floor.id = "p_user2_floor";
        p_user2_floor.attribute = "location";
        p_user2_floor.attributeType = AttributeType.STRING;
        p_user2_floor.operator = Operators.BEGIN_WITH;
        p_user2_floor.valueString = this.floor;

        r_user2_on_floor.predicateList.add(p_user2);
        r_user2_on_floor.predicateList.add(p_user2_floor);

        recordRuleList.add(r_user2_on_floor);

        return recordRuleList;
    }

    @Override
    IRuleMerger merger() {

        return new AndTwoMerger(this,"r_user1_on_floor", "r_user2_on_floor");
    }
}
