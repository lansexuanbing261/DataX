package com.alibaba.datax.plugin.writer.kuduwriter.util;

import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class KuduUtil {
    private static final Logger logger = LoggerFactory.getLogger(KuduUtil.class);

    private static final String KUDU_MASTER = "kudu_master";

    public static KuduClient createClient(String kudu_master){
        return new KuduClient.KuduClientBuilder(kudu_master).build();
    }
//    private static KuduClient client = new KuduClient.KuduClientBuilder(
//            KUDU_MASTER).build();

    public static Operation WrapperKuduOperation(KuduColumn entity, Operation operate) {

        Type type = entity.getType();
        Integer index = entity.getIndex();
        Object rawData = entity.getRawData();

        logger.info("kudu操作对象包装，列索引:{},列值:{},类型:{}", index, rawData,type);
        //logger.info("kudu操作对象包装，列值:{}", rawData);

        if (type.equals(Type.BINARY)) {
        }
        if (type.equals(Type.STRING)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addString(index, String.valueOf(rawData));
            }
        }
        if (type.equals(Type.BOOL)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addBoolean(index, Boolean.valueOf(rawData.toString()));
            }
        }
        if (type.equals(Type.DOUBLE)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addDouble(index, Double.valueOf(rawData.toString()));
            }
        }
        if (type.equals(Type.FLOAT)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addFloat(index, Float.valueOf(rawData.toString()));
            }
        }
        if (type.equals(Type.INT8)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addByte(index, Byte.valueOf(rawData.toString()));
            }
        }
        if (type.equals(Type.INT16)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addShort(index, Short.valueOf(rawData.toString()));
            }
        }
        if (type.equals(Type.INT32)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addInt(index, Integer.valueOf(rawData.toString()));
            }
        }
        if (type.equals(Type.INT64)) {
            //Integer kuduRawData = (Integer) rawData;
            //Integer kuduRawData = Integer.valueOf(rawData.toString());
            if (isSetLogic(entity, operate)) {
                //operate.getRow().addLong(index, kuduRawData);
                operate.getRow().addLong(index, Integer.valueOf(rawData.toString()));
            }
        }
        if (type.equals(Type.UNIXTIME_MICROS)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addLong(index, Long.valueOf(rawData.toString()));
            }
        }
        return operate;
    }

    /**
     * 返回查询的一行 map
     *
     * @param row
     * @param entitys
     * @return
     */
    public static Map<Integer, Object> getRowsResult(RowResult row, List<KuduColumn> entitys) {
        Map<Integer, Object> result = new HashMap();
        for (KuduColumn entity : entitys) {
            if (entity.getType() != null) {
                switch (entity.getType()) {
                    case BOOL:
                        result.put(entity.getIndex(), row.getBoolean(entity.getIndex()));
                        break;
                    case BINARY:
                        result.put(entity.getIndex(), row.getBinary(entity.getIndex()));
                        break;
                    case STRING:
                        result.put(entity.getIndex(), row.getString(entity.getIndex()));
                        break;
                    case INT8:
                        result.put(entity.getIndex(), row.getByte(entity.getIndex()));
                        break;
                    case INT16:
                        result.put(entity.getIndex(), row.getShort(entity.getIndex()));
                        break;
                    case INT32:
                        result.put(entity.getIndex(), row.getInt(entity.getIndex()));
                        break;
                    case INT64:
                        result.put(entity.getIndex(), row.getLong(entity.getIndex()));
                        break;
                    case DOUBLE:
                        result.put(entity.getIndex(), row.getDouble(entity.getIndex()));
                        break;
                    case FLOAT:
                        result.put(entity.getIndex(), row.getFloat(entity.getIndex()));
                        break;
                    case UNIXTIME_MICROS:
                        result.put(entity.getIndex(), row.getLong(entity.getIndex()));
                        break;
                }
            }
        }
        return result;
    }

    /**
     * 通用方法
     *
     * @param entity
     * @param operate
     * @param session
     * @return
     * @throws KuduException
     */
    public static OperationResponse operate(KuduRow entity, Operation operate, KuduSession session) throws KuduException {
        for (KuduColumn column : entity.getRows()) {
            KuduUtil.WrapperKuduOperation(column, operate);
        }
        OperationResponse apply = session.apply(operate);
        return apply;
    }


//    public static OperationResponse operate(Map<Integer,Object> row, Operation operate, KuduSession session) throws KuduException {
//        List<KuduColumn> columns = null;
//        columns.add(row.values());
//        for (KuduColumn column : columns) {
//            KuduUtil.WrapperKuduOperation(column, operate);
//        }
//        OperationResponse apply = session.apply(operate);
//        return apply;
//    }
    /**
     * 返回column的string list
     *
     * @param entitys
     * @return
     */
    public static List<Integer> getIndexs(List<KuduColumn> entitys) {
        List<Integer> result = new ArrayList();
        for (KuduColumn entity : entitys) {
            if (entity.isSelect()) {
                result.add(entity.getIndex());
            }
        }
        return result;
    }

//    /**
//     * 设置条件
//     *
//     * @param kuduTable
//     * @param entitys
//     * @param kuduScannerBuilder
//     */
//    public static void setKuduPredicates(KuduTable kuduTable, List<KuduColumn> entitys, KuduScanner.KuduScannerBuilder kuduScannerBuilder) {
//        for (KuduColumn entity : entitys) {
//            if (entity.getComparisonOp() != null) {
//                KuduPredicate kuduPredicate = null;
//                switch (entity.getType()) {
//                    case BOOL:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (Boolean) entity.getComparisonValue());
//                        break;
//                    case FLOAT:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (Float) entity.getComparisonValue());
//                        break;
//                    case DOUBLE:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (Double) entity.getComparisonValue());
//                        break;
//                    case BINARY:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (byte[]) entity.getComparisonValue());
//                        break;
//                    case STRING:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (String) entity.getComparisonValue());
//                        break;
//                    case UNIXTIME_MICROS:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (Long) entity.getComparisonValue());
//                        break;
//                    case INT64:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (Long) entity.getComparisonValue());
//                        break;
//                    case INT32:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (Integer) entity.getComparisonValue());
//                        break;
//                    case INT16:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (Short) entity.getComparisonValue());
//                        break;
//                    case INT8:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (byte[]) entity.getComparisonValue());
//                        break;
//                    default:
//                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getIndex()), entity.getComparisonOp(), (String) entity.getComparisonValue());
//                        break;
//                }
//                kuduScannerBuilder.addPredicate(kuduPredicate);
//            }
//        }
//    }


    /**
     * 如果是update事件并且是更新字段就设置，如果非update事件都设置
     * 如果是delete事件是主键就设置，不是主键就不设置
     *
     * @param entity
     * @param operate
     * @return
     */
    public static boolean isSetLogic(KuduColumn entity, Operation operate) {
        //return ((operate instanceof Update && entity.isUpdate()) || (operate instanceof Update && entity.isPrimaryKey())) || (operate instanceof Delete && entity.isPrimaryKey()) || (!(operate instanceof Update) && !(operate instanceof Delete));
        return ((operate instanceof Update && entity.isUpdate()) || (!(operate instanceof Update) && !(operate instanceof Delete)));
    }

    public static List<OperationResponse> close(KuduSession session, KuduClient client) {
        if (null != session) {
            try {
                session.flush();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        List<OperationResponse> responses = null;
        if (null != session) {
            try {
                responses = session.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        if (null != client) {
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        return responses;
    }

    public static void close(KuduScanner build, KuduClient client) {
        if (null != build) {
            try {
                build.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        if (null != client) {
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }
}
