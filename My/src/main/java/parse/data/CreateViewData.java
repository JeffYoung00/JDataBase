package parse.data;

import lombok.ToString;

@ToString
public class CreateViewData {
    private String viewName;
    private SelectData  selectData;
    public CreateViewData(String viewName,SelectData selectData){
        this.viewName=viewName;
        this.selectData=selectData;
    }
}
