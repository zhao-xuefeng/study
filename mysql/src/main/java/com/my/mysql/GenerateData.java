package com.my.mysql;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class GenerateData {
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public  List< Order> getData() throws SQLException {
        String userName = "root";
        String password = "123456";
        String url = "jdbc:mysql://192.168.100.12:3306/flink";
        Connection conn = DriverManager.getConnection(url, userName, password);
        PreparedStatement pstmt = conn.prepareStatement("select * from order_tb");
        ResultSet resultSet = pstmt.executeQuery();

        DataUtil dataUtil = new DataUtil();
        List< Order> orderList = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<>();
            Order order = new Order();
            order.setUserId(resultSet.getString("userId"));
            order.setUserName(resultSet.getString("userName"));
            order.setUserAddr(resultSet.getString("userAddr"));
            order.setProductPrice(resultSet.getString("productPrice"));
            order.setProductName(resultSet.getString("productName"));
            order.setProductType(resultSet.getString("productType"));
            order.setManufacturers(resultSet.getString("manufacturers"));
            order.setDate(resultSet.getString("date"));
//            map.put("useId",resultSet.getString("userId"));
//            map.put("userName",resultSet.getString("userName"));
//            map.put("userAddr", resultSet.getString("userAddr"));
//            map.put("productName", resultSet.getString("productName"));
//            map.put("productPrice", resultSet.getString("productPrice"));
//            map.put("productType", resultSet.getString("productType"));
//            map.put("manufacturers", resultSet.getString("manufacturers"));
//            map.put("date",resultSet.getString("date"));
//            orderList.add(map);
//            list2.add(mapList);
            orderList.add(order);
//            order.setList(list2);
        }
//        dataUtil.setOrderList(orderList);
        return orderList;
    }

    public static void main(String[] args) throws SQLException {
        GenerateData generateData = new GenerateData();
        List< Order> mapList = generateData.getData();
        System.out.println(JSON.toJSONString(mapList));
        List<Order> list=JSON.parseArray(JSON.toJSONString(mapList),Order.class);
        System.out.println(list.get(1).getDate());
//        for (Map<String,Object> map:mapList.getList()){
//            System.out.println(map.get("userId")+"\t"+map.get("userName"));
//        }
    }
}
