package materialize;

import java.util.List;

public class LoserTree<T extends Comparable<T>>{
    //败者的序号,tree[i]的值在values[tree[i]]中
    private int[] tree;
    private List<T> values;
    private int K;
    private int leftOne;

    public LoserTree(List<T> arr) {
        K = arr.size();
        leftOne= arr.size();
        values = arr;
        tree = new int[K];
        createTree();
    }

    public void createTree(){
        for(int i=K-1;i>=0;i--){
            tree[i]=-1;
        }

        // 从每个叶子节点开始向上调整败者树
        // 逆序调整防止奇数个节点
        for (int i = K - 1; i >= 0; i--) {
            adjust(i);
        }
    }

    //values[current]的值做出了改变,需要相应的做出调整
    private void adjust(int current) {
        //!!
        int next = (current + K) / 2;
        while (next > 0) {
            //上一家不存在,只换上去一次,只有初始化用到
            if(tree[next]==-1){
                tree[next]=current;
                return;
            }

            //和上一个输家比较,输了换上一个输家挑战
            //null即最大值
            if ( values.get(current)==null || (values.get(tree[next])!=null&& values.get(current) .compareTo( values.get(tree[next]) )>0 )) {
                int temp = current;
                current = tree[next];
                tree[next] = temp;
            }
            next /= 2;
        }
        //winner实际上已经被换出去了
        tree[0] = current;
    }

    // 输出赢家
    public T getWinnerValue() {
        return values.get(tree[0]);
    }

    //
    public int getWinner(){
        return tree[0];
    }

    // 更新
    public void insert(T value){
        int position=getWinner();
        values.set(position,value);
        adjust(position);
    }

    //
    public void removeWinner(){
        int position=getWinner();
        values.set(position,null);
        adjust(position);
        leftOne--;
    }

    public boolean isEmpty(){
        return leftOne==0;
    }
}