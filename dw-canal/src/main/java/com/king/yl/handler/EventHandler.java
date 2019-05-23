package com.king.yl.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.CaseFormat;
import com.king.yl.constant.GmallConstant;
import com.king.yl.util.KafkaSenderUtil;

import java.util.List;

public class EventHandler {

    /***
     * 根据表名和事件类型，来发送kafka
     * @param tableName
     * @param eventType
     * @param rowList
     */

    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowList) {
        //判断业务类型
        if ("order_info".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT) && rowList != null && rowList.size() > 0) {  //下单业务
            for (CanalEntry.RowData rowData : rowList) {  //遍历行集

                JSONObject jsonObject = new JSONObject();

                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();  //得到列集

                for (CanalEntry.Column column : afterColumnsList) {  //遍历列集

//                    System.out.println(column.getName() + ":::::" + column.getValue());

                    String property = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());

                    System.out.println(property + ":::::" + column.getValue());

                    jsonObject.put(property, column.getValue());
                }

                KafkaSenderUtil.send(GmallConstant.KAFKA_TOPIC_ORDER, jsonObject.toJSONString());


            }

        }


    }


}
