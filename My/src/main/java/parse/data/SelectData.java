package parse.data;

import lombok.Getter;
import lombok.ToString;
import predicate.Predicate;

import java.util.List;

@ToString
public class SelectData {
    @Getter private List<String> resultFields;
    @Getter private List<String> tableNames;
    @Getter private Predicate predicate;
    public SelectData(List<String >resultFields,List<String>tableNames,Predicate predicate){
        this.resultFields=resultFields;
        this.tableNames=tableNames;
        this.predicate=predicate;
    }
}
