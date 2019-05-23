package com.king.yl.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.king.yl.handler.EventHandler;

import java.net.InetSocketAddress;
import java.util.List;

/***
 * 从MySQL中获取数据
 */
public class CanalClientApp {

    public static void main(String[] args) {
        // 创建连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop101", 11111), "example", "", "");
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall1128.order_info");
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size == 0) {
                System.out.println("没有数据 ，休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        //解析（反序列化） 行变化数据
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //提取行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        String tableName = entry.getHeader().getTableName();
                        //执行业务方法
                        EventHandler.handle(tableName, eventType, rowDatasList);


                    }


                }


            }


        }


    }

}
