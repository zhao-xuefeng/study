package code;

public class Test2 {
    public static int febonaccis(int i){
        if(i == 1 || i == 2){
            return 1;
        }else{
            return febonaccis(i-1) + febonaccis(i - 2);
        }
    }
    public static void main(String[] args) {
        System.out.println(febonaccis(5));
    }
}
