package transaction.concurrency;

import file.BlockId;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

public class WaitForGraph {
    private HashMap<Integer, BlockId> waitFor=new HashMap<>();

    //事务a等待b,检测环路
    //return false存在环路
    public synchronized boolean put(int a,BlockId b){
        List<Integer> txs=Concurrency.lockTable.getTxs(b);
        if(!testList(a,txs)){
            return false;
        }
        waitFor.put(a,b);
        return true;
    }

    //检测这些txs是否在等待a
    //每次插入都会检测环路,所以应该不用担心无限循环...
    public boolean testList(int a,List<Integer> list){
        if(list==null){
            return true;
        }
        if(list.contains(a)){
            return false;
        }
        for(Integer i:list){
            BlockId b=waitFor.get(i);
            if(b==null){
                continue;
            }
            if(!testList(a, Concurrency.lockTable.getTxs(b))){
                return false;
            }
        }
        return true;
    }

    //事务a结束了等待
    public synchronized void remove(int a){
        waitFor.remove(a);
    }
}
