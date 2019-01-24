package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.kuduwriter.util.*;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.shaded.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(KuduWriter.Job.class);

        private Configuration originalConfig;
        private String kudu_master;
        private String kudu_table;
        //private List<Configuration> columns;
        private String writeMode;
//        private HashSet<String> endFiles = new HashSet<String>();//最终文件全路径

        //private KuduUtil kuduUtil = null;
        private KuduWriter.Job KuduWriterJob;
        private KuduSession session = null;
        private KuduClient client = null;

        @Override
        public void init() {
            this.originalConfig = this.getPluginJobConf();
            //this.validateParameter();

            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null == writeMode) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.CONF_ERROR,
                                String.format(
                                        "写入模式(writeMode)配置错误. 请检查您的配置并作出修改",
                                        writeMode));
            }

            System.out.println("writerMode" + writeMode);
            //this.KuduWriterJob = new KuduWriter.Job();
            //this.KuduWriterJob.validateParameter();
            //this.init();
        }

        private void validateParameter() {

        }

        @Override
        public void prepare() {
            //this.KuduWriterJob.prepare();
        }

        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void post() {

            // this.KuduWriterJob.post();

        }

        @Override
        public void destroy() {
            if (session != null && client != null) {
                KuduUtil.close(session, client);
            }
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(KuduWriter.Task.class);

        private Configuration writerSliceConfig;
       // private KuduWriter.Task KuduWriterTask;
//        private KuduClient kuduClient;

        private String kudu_master;
        private String kudu_table;
        //private String batchSize;
        private String writeMode ;
        private static int BATCH_SIZE = 1024;
        //        private List<KuduRow> kudurows;
//        private List<KuduColumn> kuduColumns;
        private List<KuduFieldType> typeList;
        private List<KuduColumn> columnList;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            //kudu_master = Key.getKuduMaster(writerSliceConfig.getConfiguration("kudu_master"));
            //kudu_table = Key.getKuduTable(writerSliceConfig.getConfiguration("kudu_table"));
            //writeMode = Key.getWriteMode(writerSliceConfig.getConfiguration("writeMode"));
            kudu_master = writerSliceConfig.getString("kudu_master");
            kudu_table = writerSliceConfig.getString("kudu_table");
            writeMode = writerSliceConfig.getString("writeMode");
            //this.init();
        }

        @Override
        public void prepare() {
            //System.out.println("writerSliceConfig " + writerSliceConfig);
            //System.out.println("kudu_master " + kudu_master);
            //System.out.println("kudu_table " + kudu_table);
            //System.out.println("writeMode " + writeMode);

        }

        public void startWrite(RecordReceiver recordReceiver) {
            Map<Integer, Object> data = null;
            if (Strings.isNullOrEmpty(kudu_master) || Strings.isNullOrEmpty(kudu_table) || Strings.isNullOrEmpty(writeMode)
                    || kudu_master == null || kudu_table == null || writeMode == null) {
                throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
                        KuduWriterErrorCode.ILLEGAL_VALUE.getDescription());
            }

            KuduClient client = KuduUtil.createClient(kudu_master);
            List<Record> writerBuffer = new ArrayList<Record>(this.BATCH_SIZE);
           // KuduRow kuduRow = new KuduRow();
            List<KuduRow> entity = new ArrayList<KuduRow>();
            Record record;
            KuduRow rawData = new KuduRow();
            List<KuduColumn> kuduColumnList = new ArrayList<KuduColumn>();
            while ((record = recordReceiver.getFromReader()) != null) {
                LOG.info("Record Raw: {}", record.toString());

                //类型转化
                try {
                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        Column column = record.getColumn(i);
                        //System.out.println("step "+ column);
                        int index = i;
                        Column.Type columnType = record.getColumn(i).getType();
                       // System.out.println("step-0 "+ columnType);
                        record.getColumn(i).getRawData();
                       // System.out.println("step-1 "+ record.getColumn(i).getRawData());
                        KuduColumn kr = new KuduColumn();
                        //kr.setIndex(i).setRawData(column.getRawData()).setType(Type.STRING);

                        //KuduFieldType kuduColumnType;

                        if (index < 0) {
                            System.out.println("One row's columns is null...");
                        } else {
                            System.out.println("column_type is: "+ columnType);
                            switch (columnType) {
                                case LONG:
                                    kr.setIndex(i).setRawData(column.getRawData()).setType(Type.INT64).getType();
                                   // System.out.println("kr " + kr);
                                    kuduColumnList.add(kr);
                                  //  System.out.println("transform_before: " +columnType + "transform_after: " +kr.getType());
                                    break;
                                    //kr.setRawData(record.getColumn(i).getRawData());
                                case INT:
                                    kr.setIndex(i).setRawData(column.getRawData()).setType(Type.INT32).getType();
                                  //  System.out.println("kr " + kr);
                                    kuduColumnList.add(kr);
                                   // System.out.println("transform_before: " +columnType + "transform_after: " +kr.getType());
                                    break;
                                case DOUBLE:
                                    kr.setIndex(i).setRawData(column.getRawData()).setType(Type.DOUBLE).getType();
                                   // System.out.println("kr " + kr);
                                    kuduColumnList.add(kr);
                                  //  System.out.println("transform_before: " +columnType + "transform_after: " +kr.getType());
                                    break;
                                    //kr.setRawData(record.getColumn(i).getRawData());
                                case BOOL:
                                    kr.setIndex(i).setRawData(column.getRawData()).setType(Type.BOOL).getType();
                                   // System.out.println("kr " + kr);
                                    kuduColumnList.add(kr);
                                   // System.out.println("transform_before: " +columnType + "transform_after: " +kr.getType());
                                    break;
                                    //kr.setRawData(record.getColumn(i).getRawData());
                                case STRING:
                                    kr.setIndex(i).setRawData(column.getRawData()).setType(Type.STRING).getType();
                                   // System.out.println("kr " + kr);
                                    kuduColumnList.add(kr);
                                   // System.out.println("transform_before: " +columnType + "transform_after: " +kr.getType());
                                    break;
                                    //kr.setRawData(record.getColumn(i).getRawData());
                                case DATE:
                                    kr.setIndex(i).setRawData(column.getRawData()).setType(Type.UNIXTIME_MICROS).getType();
                                  //  System.out.println("kr " + kr);
                                    kuduColumnList.add(kr);
                                  //  System.out.println("transform_before: " +columnType + "transform_after: " +kr.getType());
                                    break;
                                    //kr.setRawData(record.getColumn(i).getRawData());
                                case BYTES:
                                    kr.setIndex(i).setRawData(column.getRawData()).setType(Type.BINARY).getType();
                                    //System.out.println("kr " + kr);
                                    kuduColumnList.add(kr);
                                   // System.out.println("transform_before: " +columnType + "transform_after: " +kr.getType());
                                    break;
                                    //kr.setRawData(record.getColumn(i).getRawData());
                                default:
                                    System.out.println("DataX transform data's type is not found...");
                                    break;
                                    //kr.setIndex(i).setRawData(column.getRawData()).setType(Type.STRING).getType();
                                    //kr.setRawData(record.getColumn(i).getRawData());
                            }
                            //System.out.println("kr " + kr);
                            //kuduColumnList.add(kr);
                            //System.out.println("kuduColumnList "+ kuduColumnList);
                        }
                        //data.put(i,kr);
                        //System.out.println("data " + data);
                        //List<KuduRow> entity = new ArrayList<KuduRow>();
                    }
                    rawData.setRows(kuduColumnList);
                    //System.out.println("rawData " + rawData);
                    entity.add(rawData);

                    //System.out.println("record " + record.toString());
                    //System.out.println("entity " + entity.toString());
                   // System.out.println("kudu_table " + kudu_table);
                   // System.out.println("client " + client);

                    if (writeMode.equals("insert")) {
                        // if (writerBuffer.size() >= this.BATCH_SIZE) {
                        //doBatchInsert(col,writerBuffer,mongodbColumnMeta);
                        try {
                            //KuduAgent.insert(kudu_table, client, entity);
                            //System.out.println("insert_rows " + rawData);
                            //KuduAgent.insert(kudu_table, client, rawData);
                            KuduAgent.insert(kudu_table, client, entity);
                            System.out.println("insert single successfully");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //rawData.clear();
                        // }
                    } else if (writeMode.equals("update")) {
                        if (writerBuffer.size() >= this.BATCH_SIZE) {
                            try {
                                KuduAgent.update(kudu_table, client, entity);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            entity.clear();
                        }
                    } else if (writeMode .equals("delete") ){
                        if (writerBuffer.size() >= this.BATCH_SIZE) {
                            try {
                                KuduAgent.delete(kudu_table, client, entity);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            entity.clear();
                        }
                    }else {
                        System.out.println("please check writerMode...");
                    }
                } catch (IllegalArgumentException e) {
                    LOG.warn("Found dirty data.", e);
                    // collector.collectDirtyRecord(record, e.getMessage());
                }
            }

//            if (writeMode == "insert"){
//                while((record = recordReceiver.getFromReader()) != null) {
//                    writerBuffer.add(record);
//                    if(writerBuffer.size() >= this.BATCH_SIZE) {
//                        //doBatchInsert(col,writerBuffer,mongodbColumnMeta);
//                        KuduAgent.insert(kudu_table,client,entity);
//                        writerBuffer.clear();
//                    }
//                }
//                if(!writerBuffer.isEmpty()) {
//                    //doBatchInsert(col,writerBuffer,mongodbColumnMeta);
//                    writerBuffer.clear();
//                }
//            }else if (writeMode == "update"){
//                while((record = recordReceiver.getFromReader()) != null) {
//                    writerBuffer.add(record);
//                    if(writerBuffer.size() >= this.BATCH_SIZE) {
//                        //doBatchInsert(col,writerBuffer,mongodbColumnMeta);
//                        KuduAgent.update(kudu_table,client,writerBuffer);
//                        writerBuffer.clear();
//                    }
//                }
//                if(!writerBuffer.isEmpty()) {
//                    //doBatchInsert(col,writerBuffer,mongodbColumnMeta);
//                    writerBuffer.clear();
//                }
//            }else if (writeMode == "delete"){
//                while((record = recordReceiver.getFromReader()) != null) {
//                    writerBuffer.add(record);
//                    if(writerBuffer.size() >= this.BATCH_SIZE) {
//                        //doBatchInsert(col,writerBuffer,mongodbColumnMeta);
//                        KuduAgent.delete(kudu_table,client,writerBuffer);
//                        writerBuffer.clear();
//                    }
//                }
//                if(!writerBuffer.isEmpty()) {
//                    //doBatchInsert(col,writerBuffer,mongodbColumnMeta);
//                    writerBuffer.clear();
//                }
//            }else{
//                throw DataXException
//                        .asDataXException(
//                                DBUtilErrorCode.CONF_ERROR,
//                                String.format(
//                                        "写入模式(writeMode)配置错误. 只支持insert update 和delete，请检查您的配置并作出修改。",
//                                        writeMode));
//            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
//            try {
//                client.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }
    }
}