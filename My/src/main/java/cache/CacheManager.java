package cache;

import file.BlockId;
import file.FileManager;
import log.LogManager;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * page替换策略
 */
public class CacheManager {

    private static double Young_Percent=0.625;
    private static final long MAX_WAIT_TIME=10000;

    private int Cache_SIZE;
    private int freeCaches;
    private int youngSize;

    //old size 没有必要存在,可以全放进old list, 只是young list的大小需要控制
    //int oldSize;

    private Map<BlockId,Cache> youngList=new LinkedHashMap<>();
    private Map<BlockId,Cache> oldList=new LinkedHashMap<>();
    private Queue<Cache> caches=new LinkedList<>();

    public CacheManager(FileManager fileManager, LogManager logManager,int Cache_SIZE){
        this.Cache_SIZE=Cache_SIZE;
        youngSize=(int)(Cache_SIZE*Young_Percent);
        for(int i=0;i<Cache_SIZE;i++){
            caches.add(new Cache(fileManager,logManager));
        }
        freeCaches=Cache_SIZE;
    }

    public synchronized int getFreeCaches(){
        return freeCaches;
    }

    public synchronized void flushAll(){
        for(Cache cache:youngList.values()){
            if(cache.isModified()){
                cache.flush();
            }
        }
        for(Cache cache:oldList.values()){
            if(cache.isModified()){
                cache.flush();
            }
        }
    }

    /**
     * 生产者
     */
    public synchronized void unpin(Cache cache){
        cache.unpin();
        if(!cache.isPinned()){
            freeCaches++;
            notifyAll();
        }
    }

    /**
     * 消费者
     */
    public synchronized Cache pin(BlockId blockId){
        try{
            long timeStamp=System.currentTimeMillis();
            Cache cache=pinOneTimes(blockId);
            while(cache==null&& System.currentTimeMillis()-timeStamp<=MAX_WAIT_TIME){
                wait(MAX_WAIT_TIME);
                cache=pinOneTimes(blockId);
            }
            if(cache==null){
                throw new NoFreeCacheException();
            }
            //first pin
            if(!cache.isPinned()){
                freeCaches--;
            }
            cache.pin();
            return cache;
        }catch(InterruptedException e){
            throw new NoFreeCacheException();
        }
    }

    private Cache pinOneTimes(BlockId blockId){
        //如果存在,改变顺序
        Cache ret;
        if( (ret=youngList.remove(blockId))!=null ){
            youngList.put(blockId,ret);
            return ret;
        }
        if( (ret=oldList.remove(blockId))!=null ){
            /**
             * 满足条件可以进young list && young list中有空位 -> 进young list
             * 否则 更新old list
             */
            if(toYoung(ret) && (youngList.size()<youngSize||moveLastToOld()) ) {
                youngList.put(blockId, ret);
            } else{
                oldList.put(blockId,ret);
            }
            return ret;
        }

        //初始化中
        if (!caches.isEmpty()) {
            ret = caches.remove();
            ret.fillUp(blockId);
            oldList.put(blockId, ret);
            return ret;
        }

        //开始替换
        if(freeCaches==0){
            return null;
        }
        ret=replaceCacheFromList(oldList);
        if(ret==null){
            ret=replaceCacheFromList(youngList);
        }
        ret.fillUp(blockId);
        oldList.put(blockId,ret);
        return ret;
    }

    /**
     * 判断是否达到了进入youngList的条件
     * todo 目前随便加了个条件,每一分钟访问一次就算高频访问
     */
    private boolean toYoung(Cache cache){
        LocalDateTime time=cache.getStartDate();
        long timeStamp=System.currentTimeMillis();
        int diff=(int)(timeStamp/1000-time.toEpochSecond(ZoneOffset.UTC));
        int pins=cache.getTotalPins();
        return diff<=60*pins;
    }

    /**
     * 将young list中的cache淘汰进old list
     * 返回是否成功淘汰
     */
    private boolean moveLastToOld(){
        Cache cache= replaceCacheFromList(youngList);
        if(cache==null){
            return false;
        }
        oldList.put(cache.getBlockId(),cache);
        return true;
    }

    /**
     * 从young list中找到被淘汰的entry并返回
     */
    Cache replaceCacheFromList(Map<BlockId,Cache>list){
        Iterator<Cache> iterator=list.values().iterator();
        while(iterator.hasNext()){
            Cache cache =iterator.next();
            if(!cache.isPinned()){
                list.remove(cache.getBlockId());
                return cache;
            }
        }
        return null;
    }
}