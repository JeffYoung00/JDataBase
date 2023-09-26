package record;

import server.DatabaseException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Layout {
    //field name to offset
    private Map<String,Integer> offsetMap=new HashMap<>();
    private Map<String,Integer> typeMap=new HashMap<>();
    private int recordSize;

    public Layout(Schema schema){
        int pos=0;
        List<Field> fieldList= schema.getFieldList();
        for(Field field:fieldList){
            offsetMap.put(field.getName(),pos);
            if(field.getType()==Field.Integer){
                pos+=4;
                typeMap.put(field.getName(), Field.Integer);
            } else if (field.getType()==Field.String) {
                pos+=4;
                pos+=field.getLen();
                typeMap.put(field.getName(), Field.String);
            }else{
                throw new DatabaseException("unsupported field type");
            }
        }
        pos+= RecordPage.Flag_Size;
        recordSize=pos;
    }

    public int getOffset(String fieldName){
        Integer ret=offsetMap.get(fieldName);
        if(ret==null){
            throw new DatabaseException("wrong field name");
        }
        return ret;
    }

    public int getType(String fieldName){
        Integer ret=typeMap.get(fieldName);
        if(ret==null){
            throw new DatabaseException("wrong field name");
        }
        return ret;
    }

    public int getRecordSize(){
        return recordSize;
    }

    public boolean hasField(String fieldName){
        return offsetMap.get(fieldName)!=null;
    }

    public Set<String> getFieldSet(){
        return offsetMap.keySet();
    }

    public static int countRecordSize(Schema schema){
        int pos=0;
        List<Field> fieldList= schema.getFieldList();
        for(Field field:fieldList){
            if(field.getType()==Field.Integer){
                pos+=4;
            } else if (field.getType()==Field.String) {
                pos+=4;
                pos+=field.getLen();
            }else{
                throw new DatabaseException("unsupported field type");
            }
        }
        pos+=RecordPage.Flag_Size;
        return pos;
    }
}
