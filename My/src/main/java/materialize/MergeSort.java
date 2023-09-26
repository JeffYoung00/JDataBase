package materialize;

import file.FileManager;
import predicate.Constant;
import scan.Scan;
import record.*;
import transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class MergeSort<T extends Comparable<T>>{

    Transaction transaction;
    String sortFieldName;
    int mRoute;
    Layout layout;
    int recordsCountPreBlock;
    Schema schema;

    TableScan lastTableScan;

    public MergeSort(Transaction transaction, Schema schema, String sortFieldName, Scan scan, int mRoute){
        this.transaction=transaction;
        this.sortFieldName=sortFieldName;
        this.mRoute=mRoute;
        this.schema=schema;

        this.layout=new Layout(schema);
        this.recordsCountPreBlock= FileManager.BLOCK_SIZE/layout.getRecordSize();

        List<TemporaryTable> runs=splitIntoRuns(scan);
        while(runs.size()!=1){
            runs=merge(runs);
        }
        lastTableScan =runs.get(0).open();
    }

    /**
     * 用temporary list作为返回值, 因为不能同时打开所有的table scan
     * @param scan
     * @return
     */
    private List<TemporaryTable> splitIntoRuns(Scan scan){
        List<TemporaryTable> runs=new ArrayList<>();

        boolean end=false;
        while(!end){
            //得到一个run的所有记录并排序
            List<FieldValueRecordIdPair<T>> pairs=new ArrayList<>();
            for(int i=0;i<recordsCountPreBlock*mRoute ;i++){
                if(!scan.hasNext()){
                    end=true;
                    break;
                }
                List<Constant<?>> values=new ArrayList<>(schema.getFieldList().size());
                for(Field field: schema.getFieldList()){
                    values.add( scan.getValue(field.getName()) );
                }
                pairs.add(new FieldValueRecordIdPair<T>((T)scan.getValue(sortFieldName).getValue(),values));
            }

            //避免空temp table
            if(pairs.size()==0){
                break;
            }
            pairs.sort((a,b)->a.fieldValue.compareTo(b.fieldValue));

            //用一个table scan作为run
            TemporaryTable temporaryTable=new TemporaryTable(transaction,layout);
            TableScan tempScan=temporaryTable.open();
            runs.add(temporaryTable);

            //复制内容
            for(int i=0;i<pairs.size();i++){
                tempScan.insert();
                for(int j=0;j<schema.getFieldList().size();j++){
                    tempScan.setValue( schema.getFieldList().get(j).getName(), pairs.get(i).values.get(j),false);
                }
            }
            tempScan.close();
        }
        return runs;
    }

    private List<TemporaryTable> merge(List<TemporaryTable>runs){
        List<TemporaryTable> newRuns=new ArrayList<>();
        for(int i = 0; i <runs.size();){
            int end= Math.min(i + mRoute, runs.size());

            //open
            List<TableScan> in=new ArrayList<>();
            for(int j=i;j<end;j++){
                in.add(runs.get(i+j).open());
            }
            TemporaryTable newTemp=new TemporaryTable(transaction,layout);
            TableScan out = newTemp.open();
            newRuns.add(newTemp);

            //merge
            mergeOnce(in,out);

            //close
            for(int j=i;j<end;j++){
                in.get(j).close();
            }
            out.close();

            i=end;
        }

        //clear old runs
        for(TemporaryTable temporaryTable:runs){
            transaction.emptyTempFile(temporaryTable.getFileName());
        }
        return newRuns;
    }

    private void mergeOnce(List<TableScan>in,TableScan out){
        ArrayList<T> initList=new ArrayList<>();
        for(int i=0;i<in.size();i++){
            in.get(i).hasNext();
            initList.add((T) in.get(i).getValue(sortFieldName).getValue());
        }
        LoserTree<T> loserTree=new LoserTree<T>( initList);

        byte[] bytes=new byte[layout.getRecordSize()+RecordPage.Flag_Size];
        while(loserTree.isEmpty()){
            //复制
            int winner = loserTree.getWinner();
            in.get(winner).getRecordByte(bytes);
            out.setRecordByte(bytes);

            if(in.get(winner).hasNext()){
                loserTree.insert((T)in.get(winner).getValue(sortFieldName).getValue() );
            }else{
                loserTree.removeWinner();
            }
        }
    }


    //交给table scan
    public TableScan getLastTableScan(){
        return lastTableScan;
    }


    private static class FieldValueRecordIdPair<T>{
        private static int recordSize;
        T fieldValue;
        List<Constant<?>> values;
        FieldValueRecordIdPair(T fieldValue,List<Constant<?>>values){
            this.fieldValue=fieldValue;
            this.values=values;
        }
    }
}

