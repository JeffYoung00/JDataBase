package parse.data;

import lombok.Getter;
import record.Schema;

@Getter
public class CreateTableData {

    private String tableName;
    private Schema schema;

    public CreateTableData(String tableName,Schema schema){
        this.tableName=tableName;
        this.schema=schema;
    }
}
