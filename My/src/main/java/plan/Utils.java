package plan;

public class Utils {

    //用于multi buffer product 计算趟数
    public static int calculateRunsByFactor(int size,int factor){
        if(size<=2){
            return 1;
        }
        return (int)Math.ceil( (double)size/factor );
    }

    //用于merge sort计算趟数
    public static int calculateRunsByRoot(int size,int root){
        if(size<=2){
            return 1;
        }
        return (int)Math.ceil( Math.log(size)/Math.log(root) );
    }

    public static int findFactor(int size,int available){
        //除非size太小,否则都应该至少提供2个buffer
        if(size<2){
            return 1;
        }
        if(available<2){
            return 2;
        }

        int i=0;
        int factor;
        double s=size;
        do{
            i++;
            factor = (int) Math.ceil( s/i );
        }while(factor>available);
        return factor;
    }

    public static int findRoot(int size,int available){
        if(size<2){
            return 1;
        }
        if(available<2){
            return 2;
        }

        int i=0;
        int root;
        do{
            i++;
            root = (int)Math.ceil(Math.pow(size, 1.0/i));
        }while(root>available);
        return root;
    }

    //增加条件,root需要是2的次方
    public static int findRoot2(int size,int available){
        if(size<2){
            return 1;
        }
        if(available<2){
            return 2;
        }

        int i=0;
        int root;
        do{
            i++;
            root = (int)Math.ceil(Math.pow(size, 1.0/i));
            root= (int)Math.ceil( Math.log(root) );
        }while(root>available);
        return root;
    }
}
