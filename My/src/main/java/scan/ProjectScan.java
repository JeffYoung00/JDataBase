package scan;

import predicate.Constant;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ProjectScan implements Scan{

    private Scan scan;
    private Set<String> fieldNames;

    public ProjectScan(Scan scan,List<String> fieldNames){
        this.scan=scan;
        this.fieldNames= new HashSet<>(fieldNames);
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean hasNext() {
        return scan.hasNext();
    }

    @Override
    public Constant<?> getValue(String filedName){
        return scan.getValue(filedName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldNames.contains(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }
}
