package com.yami.test;

import com.yami.bean.Message;
import com.yami.constants.Constants;
import com.yami.dao.HBaseDao;
import com.yami.utils.HBaseUtis;

import java.io.IOException;
import java.util.List;

public class TestWeiBo {
    // 初始化表
    public static void init() {
        try {
            // 创建命名空间
            HBaseUtis.createNameSpace(Constants.NAMESPACE);
            // 创建微博内容表
            HBaseUtis.createTable(Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSION, Constants.CONTENT_TABLE_CF);
            // 创建用户关系表
            HBaseUtis.createTable(Constants.RELATION_TABLE, Constants.RELATION_TABLE_VERSION, Constants.RELATION_TABLE_CF1, Constants.RELATION_TABLE_CF2);
            // 创建收件箱表
            HBaseUtis.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSION, Constants.INBOX_TABLE_CF);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 初始化
        init();
        try {
            // 1001发布微博
            HBaseDao.publishWeiBo("1001", "1001发布的第一条微博");
            // 1001关注1002
            HBaseDao.addAttends("1001", "1002");
            HBaseDao.addAttends("1003", "1001");
            // 初始化页面
            HBaseDao.getinit("1003");
            // 获取某个用户的微博
            List<Message> messages = HBaseDao.getAttendsContent("1001");
            System.out.println(messages);
            // 取消关注
            HBaseDao.removeAttends("1001", "1002");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
