package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.util.Configuration;

public final class Key {

    public static final String CONNECTION = "connect";
    public static final String KUDU_MASTER = "kudu_master";
    public static final String KUDU_TABLE = "kudu_table";
    //public static final String USERNAME = "username";
    //public static final String PASSWORD = "password";
    // must have for column
    //public static final String PRESQL = "preSql";
    public static final String COLUMN = "column";
    // must have
    public static final String WRITE_MODE = "writeMode"; //insert update delete upsert
    /**
     * 【可选】遇到空值默认跳过
     */
    public static  final String NULL_MODE = "nullMode";
    /**
     * 【可选】
     * 在writer初始化的时候，是否清空目的表
     * 如果全局启动多个writer，则必须确保所有的writer都prepare之后，再开始导数据。
     */
    public static  final String TRUNCATE = "truncate";

    /**
     * 【可选】批量写入的最大行数，默认100行
     */
    public static  final String BATCH_SIZE = "batchSize";

    public static enum ActionType {
        UNKONW,
        DELETE,
        UPDATE,
        INSERT
    }

    public static ActionType getActionType(Configuration conf) {
        String actionType = conf.getString("actionType", "insert");
        if ("delete".equals(actionType)) {
            return ActionType.DELETE;
        } else if ("insert".equals(actionType)) {
            return ActionType.INSERT;
        } else if ("update".equals(actionType)) {
            return ActionType.UPDATE;
        } else {
            return ActionType.UNKONW;
        }
    }

    //public static String getKuduMaster(Configuration conf){ return conf.getString(Key.KUDU_MASTER,"172.17.10.60:7075");}
    public static String getKuduMaster(Configuration conf){ return conf.getString("kudu_master","172.17.10.60:7075");}

    //public static String getKuduTable(Configuration conf){ return conf.getString(Key.KUDU_TABLE,"k_yc_rpt.driver_portrait_table_test2");}
    public static String getKuduTable(Configuration conf){ return conf.getString("kudu_table","k_yc_rpt.driver_portrait_table_test2");}
    public static String getWriteMode(Configuration conf){ return conf.getString("writeMode","update");}


}
