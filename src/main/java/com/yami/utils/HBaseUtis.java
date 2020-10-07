package com.yami.utils;

import com.yami.constants.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseUtis {
    /**
     * 创建命名空间
     *
     * @param ns
     */
    public static void createNameSpace(String ns) throws IOException {
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        // 创建admin对象
        Admin admin = connection.getAdmin();
        // 构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        // 创建命名空间
        admin.createNamespace(namespaceDescriptor);
        // 关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     */
    public static boolean isTableExist(String tableName) throws IOException {
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        // 创建admin对象
        Admin admin = connection.getAdmin();
        // 判断表是否存在
        boolean isExist = admin.tableExists(TableName.valueOf(tableName));
        // 关闭资源
        admin.close();
        connection.close();
        return isExist;
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param version
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, int version, String... columnFamily) throws IOException {
        //判断表是否存在
        if (columnFamily.length <= 0) {
            System.out.println("请确认列族是否传入");
            return;
        }

        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            return;
        }
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        // 创建admin对象
        Admin admin = connection.getAdmin();
        //创建表属性对象,表名需要转字节
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //创建多个列族
        for (String cf : columnFamily) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            // 设置版本
            hColumnDescriptor.setMaxVersions(version);
            descriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(descriptor);
        // 关闭资源
        admin.close();
        connection.close();
    }

}
