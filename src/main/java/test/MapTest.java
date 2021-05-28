package test;

import java.util.*;

public class MapTest {
    public static void main(String[] args) {
        int a=2;
        int b=7;
        Comparator c= new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return 0;
            }
        } ;

        if (c.compare(a,b)>0){
            System.out.println(a);
        }



//        System.out.println(c.compareTo(a));
//        Map<String,Integer> maps= new HashMap<>();
//        List<Integer>  list=new ArrayList<>();
//        maps.put("a",1);
//        maps.put("b",2);
//        maps.put("c",3);
//
//        list.add(8);
//        list.add(10);
//        list.add(12);
//        list.add(19);
//        list.remove(3);
//        for (Integer i: list) {
//           list.remove(i);
//        }
//        for (Integer i: list){
//            System.out.println(i);
//        }
//        Iterator it = list.iterator();
//        while (it.hasNext()){
//            it.remove();
//        }

    }
}
