package file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Test {
    public static void main(String[] args) throws IOException {
        RandomAccessFile file=new RandomAccessFile("test2","rws");
        file.seek(3);
        int ret=file.read(new byte[3]);
        System.out.println(ret);

    }
}
