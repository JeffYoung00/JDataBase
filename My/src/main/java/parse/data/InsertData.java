package parse.data;

import lombok.Getter;
import lombok.ToString;
import predicate.Constant;

import java.util.List;

@ToString
public class InsertData {
    @Getter private String tableName;
    @Getter private List<String> fieldNameList;
    @Getter private List<Constant<?>> valueList;
    public InsertData(String tableName,List<String>fieldNameList,List<Constant<?>>valueList){
        this.tableName=tableName;
        this.valueList=valueList;
        this.fieldNameList=fieldNameList;
    }
}
