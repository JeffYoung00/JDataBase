package record;

import lombok.Data;
import lombok.Getter;
import lombok.ToString;

//todo 建表语句,表metadata反序列化,所以不能计算
@ToString
@Getter
public class Field {
    public static int Integer=0;
    public static int String=1;

    //有的字段没有长度,所以在field catalog中存储的长度是0
    public static int Null_Length=0;


    private String name;
    private int type;
    private int len;

    public Field(String name,int type,int len){
        this.len=len;
        this.name=name;
        this.type=type;
    }

    public Field(String name,int type){
        this.len=Null_Length;
        this.name=name;
        this.type=type;
    }

}
