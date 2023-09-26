package parse.data;

import lombok.Getter;
import lombok.ToString;
import predicate.Predicate;
@ToString
public class DeleteData {
    @Getter private String tableName;
    @Getter private Predicate predicate;
    public DeleteData(String tableName,Predicate predicate){
        this.tableName=tableName;
        this.predicate=predicate;
    }
}
