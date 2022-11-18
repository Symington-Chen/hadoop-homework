import java.io.File;

public class demo {
    public static void main(String[] args) {
        File f = new File("E:\\Code\\hadoop_homework\\data\\part1in\\SPAIN\\480218newsML");
        System.out.println(f.getPath().toString().split("\\\\")[5]);
    }
}
