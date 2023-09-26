package record;

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
@ToString
public class Schema {
    @Getter private List<Field> fieldList;

    public Schema(List<Field> fields){
        this.fieldList=fields;
    }

    public Schema(){
        fieldList=new ArrayList<>();
    }

    public void addField(Field field){
        fieldList.add(field);
    }

    public void addAllFieldsIn(Schema schema){
        fieldList.addAll(schema.fieldList);
    }

    public boolean hasField(String fieldName){
        for (Field field:fieldList){
            if(field.getName().equals(fieldName)){
                return true;
            }
        }
        return false;
    }
}
