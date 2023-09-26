package metadata;

import predicate.Constant;
import record.Field;
import record.Layout;
import record.Schema;
import record.TableScan;
import server.Database;
import transaction.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TableManager {

    //field和table的最长名字
    private static final int Max_Name_Length =32;

    private Layout tableCatalogLayout;
    private Layout fieldCatalogLayout;

    private Map<String, Schema> schemaMap=new HashMap<>();
    private Map<String,Layout> tableLayoutMap=new HashMap<>();

    public TableManager(Transaction transaction){
        initCatalogLayout();
        initTableLayoutMap(transaction);
    }

    private void initCatalogLayout(){
        Schema tableCatalogSchema =new Schema();
        tableCatalogSchema.addField(new Field("tableName",Field.String,Max_Name_Length) );
        tableCatalogLayout=new Layout(tableCatalogSchema);

        Schema fieldCatalogSchema =new Schema();
        fieldCatalogSchema.addField(new Field("tableName",Field.String,Max_Name_Length) );
        fieldCatalogSchema.addField(new Field("fieldName",Field.String,Max_Name_Length));
        fieldCatalogSchema.addField(new Field("fieldType",Field.Integer));
        fieldCatalogSchema.addField(new Field("fieldLength",Field.Integer));
        fieldCatalogLayout=new Layout(fieldCatalogSchema);
    }

    private void initTableLayoutMap(Transaction transaction){

        TableScan tableCatalogScan=new TableScan(transaction,tableCatalogLayout, Database.tableCatalogName);
        while(tableCatalogScan.hasNext()){
            String tableName=(String)tableCatalogScan.getValue("tableName").getValue();
            schemaMap.put(tableName,new Schema());
        }

        TableScan fieldCatalogScan=new TableScan(transaction,fieldCatalogLayout,Database.fieldCatalogName);
        while(fieldCatalogScan.hasNext()){
            String tableName=(String)fieldCatalogScan.getValue("tableName").getValue();
            String fieldName=(String)fieldCatalogScan.getValue("fieldName").getValue();
            int fieldType=(Integer) fieldCatalogScan.getValue("fieldType").getValue();
            int fieldLength=(Integer) fieldCatalogScan.getValue("fieldLength").getValue();
            Field field=new Field(fieldName,fieldType,fieldLength);
            Schema schema =schemaMap.get(tableName);
            schema.addField(field);
        }

        for(Map.Entry<String,Schema> entry:schemaMap.entrySet()) {
            Layout layout = new Layout(entry.getValue());
            tableLayoutMap.put(entry.getKey(), layout);
        }
    }

    public boolean hasTable(String tableName){
        return tableLayoutMap.get(tableName)!=null;
    }

    public boolean hasField(String tableName,String fieldName){
        return tableLayoutMap.get(tableName).hasField(fieldName);
    }

    /**
     * 表已经存在,创建失败, return false
     */
    public boolean createTable( Transaction transaction,Schema schema,String tableName){
        if(tableLayoutMap.containsKey(tableName)){
            return false;
        }

        TableScan tableScan=new TableScan(transaction,tableCatalogLayout,Database.tableCatalogName);
        tableScan.insert();
        Constant<?>tableConst=new Constant<>(tableName);
        tableScan.setValue("tableName",tableConst);
        tableScan.close();

        TableScan fieldScan=new TableScan(transaction,fieldCatalogLayout,Database.fieldCatalogName);
        for(Field field: schema.getFieldList()){
            fieldScan.insert();
            //!! 记录table
            fieldScan.setValue("tableName",tableConst);
            fieldScan.setValue("fieldName",new Constant<>(field.getName()));
            fieldScan.setValue("fieldType",new Constant<>(field.getType()));
            fieldScan.setValue("fieldLength",new Constant<>(field.getLen()));
        }
        fieldScan.close();

        //更新layout map
        tableLayoutMap.put(tableName,new Layout(schema));

        return true;
    }

    public boolean dropTable(Transaction transaction,String tableName){
        if(!tableLayoutMap.containsKey(tableName)){
            return false;
        }

        TableScan tableScan=new TableScan(transaction,tableCatalogLayout,Database.tableCatalogName);
        while(tableScan.hasNext()){
            String storedTableName=(String) tableScan.getValue("tableName").getValue();
            if(storedTableName.equals(tableName)){
                tableScan.delete();
                break;
            }
        }
        tableScan.close();

        TableScan fieldScan=new TableScan(transaction,fieldCatalogLayout,Database.fieldCatalogName);
        while(fieldScan.hasNext()){
            String storedTableName=(String) fieldScan.getValue("tableName").getValue();
            if(storedTableName.equals(tableName)){
                fieldScan.delete();
            }
        }
        fieldScan.close();

        //更新map
        tableLayoutMap.remove(tableName);
        return true;
    }

    public Layout getLayout(String tableName){
        return tableLayoutMap.get(tableName);
    }

    public Schema getSchema(String tableName){
        return schemaMap.get(tableName);
    }

    public Set<String> getTableSet(){
        return tableLayoutMap.keySet();
    }

    public Field getFieldInfo(String tableName,String fieldName){
        Schema tableSchema=schemaMap.get(tableName);
        if(tableSchema==null){
            return null;
        }
        for(Field field:tableSchema.getFieldList()){
            if(field.getName().equals(fieldName)){
                return field;
            }
        }
        return null;
    }
}