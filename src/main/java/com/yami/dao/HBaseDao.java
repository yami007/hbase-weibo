package com.yami.dao;

import com.yami.bean.Message;
import com.yami.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDao {

    // 发布微博
    public static void publishWeiBo(String uid, String content) throws IOException {
        // 第一部分，操作微博内容表
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        // 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        // 获取当前时间戳
        long currentTimeMillis = System.currentTimeMillis();
        // 获取rowkey
        String rowKey = uid + "_" + currentTimeMillis;
        // 创建PUT对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 给put对象赋值
        put.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));
        // 执行put操作
        contentTable.put(put);

        // 第二部分，操作微博收件箱表
        // 获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        // 获取当前用户的fans列族信息
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relationTable.get(get);
        // 创建一个集合，用于存储存放微博内容表的put对象
        List<Put> inoxPutList = new ArrayList<Put>();
        // 遍历粉丝
        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inoxPutList.add(inboxPut);
        }
        if (inoxPutList.size() > 0) {
            // 获取收件箱表
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inoxPutList);
            inboxTable.close();
        }
        relationTable.close();
        contentTable.close();
        connection.close();
    }


    // 关注用户
    public static void addAttends(String uid, String... attends) throws IOException {
        // 判断被关注者是否为空
        if (attends.length <= 0) {
            System.out.println("请选择关注的用户");
            return;
        }
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 第一部分，操作用户关系表
        // 1、获取用户关系表表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        // 2、操作者put对象
        Put relationAttendsPut = new Put(Bytes.toBytes(uid));
        // 3、被操作者put集合
        List<Put> fansPutList = new ArrayList<Put>();
        // 4、添加新增的关注对象
        for (String attendUid : attends) {
            // 操作者添加关注者
            relationAttendsPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attendUid), Bytes.toBytes(attendUid));
            // 被关注者添加粉丝
            Put fansPut = new Put(Bytes.toBytes(attendUid));
            fansPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            fansPutList.add(fansPut);
        }
        // 5、入库
        fansPutList.add(relationAttendsPut);
        relationTable.put(fansPutList);

        // 第二部分：操作收件箱表
        // 1、获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        // 2、创建收件箱表的put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));
        // 3、循环attends，获取每个被关注者的近期发布的微博
        for (String attend : attends) {
            // 获取当前被关注者夹紧器发布的微博
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            //使用HTable得到resultcanner实现类的对象
            ResultScanner resultScanner = contentTable.getScanner(scan);
            long currentTimeMillis = System.currentTimeMillis();
            for (Result result : resultScanner) {
                // 添加列
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), currentTimeMillis++, result.getRow());
            }
        }
        // 存入收件箱数据
        if (!inboxPut.isEmpty()) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            // 关闭资源
            inboxTable.close();
        }
        // 关闭资源
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    // 取消关注
    public static void removeAttends(String uid, String... dels) throws IOException {
        // 判断被关注者是否为空
        if (dels.length <= 0) {
            System.out.println("请选择取关的用户");
            return;
        }
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 第一部分：操作用户关系表
        // 获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        // 创建一个集合，用于存放用户关系表的Delete对象
        List<Delete> deletes = new ArrayList<Delete>();
        // 创建操作者的delete对象
        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(del));
            Delete delete = new Delete(Bytes.toBytes(del));
            delete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            deletes.add(delete);
        }
        deletes.add(uidDelete);
        // 删除数据
        relationTable.delete(deletes);

        // 第二部分：操作收件箱表
        // 获取inbox表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        // 获取操作者的delete对象
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        // 赋值
        for (String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));
        }
        // 删除
        inboxTable.delete(inboxDelete);

        // 关闭资源
        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    // 获取登录用户的初始化页面数据
    public static void getinit(String uid) throws IOException {
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        // 获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        // 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        // 创建收件箱表get对象，并获取数据（设置最大版本）
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        // 遍历获取数据
        for (Cell cell : result.rawCells()) {
            // 构建微博内容表的get对象
            Get contentGet = new Get(CellUtil.cloneValue(cell));
            // 获取该get对象的内容
            Result contentResult = contentTable.get(contentGet);
            for (Cell contentCell : contentResult.rawCells()) {
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(contentCell)));
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(contentCell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(contentCell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(contentCell)));
            }
        }
        // 关闭资源
        contentTable.close();
        inboxTable.close();
        connection.close();
    }

    // 获取某个人的所有微博详情
    public static List<Message> getAttendsContent(String uid) throws IOException {
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        // 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        // 构建scan对象
        Scan scan = new Scan();
        // 构建过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);
        // 获取数据
        ResultScanner scanner = contentTable.getScanner(scan);
        List<Message> messages = new ArrayList<Message>();
        // 解析数据并打印
        for (Result result : scanner) {
            for (Cell contentCell : result.rawCells()) {
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(contentCell)));
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(contentCell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(contentCell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(contentCell)));
                Message message = new Message();
                String rowKey = Bytes.toString(CellUtil.cloneRow(contentCell));
                String userid = rowKey.substring(0, rowKey.indexOf("_"));
                String timestamp = rowKey.substring(rowKey.indexOf("_") + 1);
                String content = Bytes.toString(CellUtil.cloneValue(contentCell));
                message.setContent(content);
                message.setTimestamp(timestamp);
                message.setUid(userid);
                messages.add(message);
            }
        }
        // 关闭资源
        contentTable.close();
        connection.close();
        return messages;
    }
}
