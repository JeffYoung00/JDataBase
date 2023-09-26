package parse;

public class Test {
    public static void main(String[] args) {
        Parser parser=new Parser("insert into table1(field1,field2,field3,field4) values(1,23,'adf',\"hhh\")");
        System.out.println(parser.parse());
    }
}
