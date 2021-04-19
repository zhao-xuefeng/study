package com.my.mysql;

import java.io.Serializable;
import java.util.List;

public class DataUtil implements Serializable {
    private List<Order> orderList;

    public List<Order> getOrderList() {
        return orderList;
    }

    public void setOrderList(List<Order> orderList) {
        this.orderList = orderList;
    }

    @Override
    public String toString() {
        return "DataUtil{" +
                "orderList=" + orderList +
                '}';
    }
}
