package cs237;

import java.util.ArrayList;
import java.util.List;

public class UserInBuildingAndRoomEmpty extends IEventRule {

    String user, room;

    UserInBuildingAndRoomEmpty(String user, String room) {
        this.user = user;
        this.room = room;
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
        return "UserInBuildingAndRoomEmpty";
    }

    @Override
    List<IRecordRule> recordRuleList() {

        List<IRecordRule> recordRuleList = new ArrayList<>();

        RecordRule r_user_in_building = new RecordRule(this);
        r_user_in_building.id = "r_user_in_building";
        r_user_in_building.stream = "PRESENCE";

        Predicate p_user = new Predicate(r_user_in_building);
        p_user.id = "p_user";
        p_user.attribute = "semantic_entity_id";
        p_user.attributeType = AttributeType.STRING;
        p_user.operator = Operators.EQUAL;
        p_user.valueString = this.user;

        r_user_in_building.predicateList.add(p_user);

        RecordRule r_room_empty = new RecordRule(this);
        r_room_empty.id = "r_room_empty";
        r_room_empty.stream = "OCCUPANCY";

        Predicate p_room = new Predicate(r_room_empty);
        p_room.id = "p_room";
        p_room.attribute = "semantic_entity_id";
        p_room.attributeType = AttributeType.STRING;
        p_room.operator = Operators.EQUAL;
        p_room.valueString = this.room;

        Predicate p_empty = new Predicate(r_room_empty);
        p_empty.id = "p_empty";
        p_empty.attribute = "occupancy";
        p_empty.attributeType = AttributeType.INT;
        p_empty.operator = Operators.EQUAL;
        p_empty.valueInt = 0;

        r_room_empty.predicateList.add(p_room);
        r_room_empty.predicateList.add(p_empty);

        recordRuleList.add(r_user_in_building);
        recordRuleList.add(r_room_empty);

        return recordRuleList;
    }

    @Override
    IRuleMerger merger() {
        return new AndTwoMerger(this, "r_user_in_building", "r_room_empty");
    }
}
