package predicate;

import lombok.Getter;
import lombok.ToString;
import server.DatabaseException;

/**
 * @param <T> T是Integer或者String
 */
@ToString
public class Constant <T extends Comparable<T>> implements Comparable<Constant<?>>{
    @Getter private T  value;
    public Constant(T value){
        this.value=value;
    }

    @Override
    public int compareTo(Constant<?> o) {
        Object v=o.getValue();
        if(v.getClass()!=value.getClass()){
            throw new DatabaseException("error constant type compare");
        }else{
            return value.compareTo((T)o.getValue());
        }
    }

    public boolean equals(Constant<?>constant){
        if(constant==null){
            return false;
        }
        return value.equals(constant.getValue());
    }
}
