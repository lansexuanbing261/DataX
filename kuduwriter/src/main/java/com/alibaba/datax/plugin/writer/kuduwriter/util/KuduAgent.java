package com.alibaba.datax.plugin.writer.kuduwriter.util;

import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KuduAgent {
    private static final Logger logger = LoggerFactory.getLogger(KuduAgent.class);

    private static KuduClient client;
    //private final static String master = "172.17.10.62";
    private final static SessionConfiguration.FlushMode FLASH_MODE_MULT = SessionConfiguration.FlushMode.MANUAL_FLUSH;
    private final static SessionConfiguration.FlushMode FLASH_MODE_SINGLE = SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
    private final static int BUFFER_SPACE = 1000;

    /**
     * 批量插入
     *
     * @param table
     * @param client
     * @param entitys
     * @throws KuduException
     */
    public static void insert(String table, KuduClient client, List<KuduRow> entitys)throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_MULT);
            session.setMutationBufferSpace(BUFFER_SPACE);
            for (KuduRow entity : entitys) {
                Insert insert = kuduTable.newInsert();
                KuduUtil.operate(entity, insert, session);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行插入操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            //throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        }
//        finally {
//            List<OperationResponse> res = KuduUtil.close(session, client);
//        }
    }

    /**
     * 单条插入
     *
     * @param table
     * @param client
     * @param entity
     * @throws KuduException
     */
    public static void insert(String table, KuduClient client, KuduRow entity) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            System.out.println("session " + session);
            session.setFlushMode(FLASH_MODE_SINGLE);
            Insert insert = kuduTable.newInsert();
            OperationResponse operate = KuduUtil.operate(entity, insert, session);
            System.out.println("operate " + operate);
            logger.info("insert 插入数据:{}", operate.getRowError());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行插入操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            //throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        }
//        finally {
//            KuduUtil.close(session, client);
//        }
    }


//    public static void insert(String table, KuduClient client, Map<Integer,Object> row) throws KuduException {
//        KuduSession session = null;
//        try {
//            KuduTable kuduTable = client.openTable(table);
//            session = client.newSession();
//            System.out.println("session " + session);
//            session.setFlushMode(FLASH_MODE_SINGLE);
//            Insert insert = kuduTable.newInsert();
//            OperationResponse operate = KuduUtil.operate(row, insert, session);
//            System.out.println("operate " + operate);
//            logger.info("insert 插入数据:{}", operate.getRowError());
//        } catch (Exception e) {
//            e.printStackTrace();
//            logger.error("kudu执行插入操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            //throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
//        }
//    }
    /**
     * 批量更新
     *
     * @param table
     * @param client
     * @param entitys
     * @throws KuduException
     */
    public static void update(String table, KuduClient client, List<KuduRow> entitys) throws KuduException{
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_MULT);
            session.setMutationBufferSpace(BUFFER_SPACE);
            for (KuduRow entity : entitys) {
                Update update = kuduTable.newUpdate();
                KuduUtil.operate(entity, update, session);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行更新操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            //throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            List<OperationResponse> res = KuduUtil.close(session, client);
        }
    }

    /**
     * 单条更新
     *
     * @param table
     * @param client
     * @param entity
     * @throws KuduException
     */
    public static void update(String table, KuduClient client, KuduRow entity) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_SINGLE);
            Update update = kuduTable.newUpdate();
            OperationResponse operate = KuduUtil.operate(entity, update, session);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行更新操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            //throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduUtil.close(session, client);
        }
    }

    /**
     * 批量删除 删除只能是主键
     *
     * @param table
     * @param client
     * @param entitys
     * @throws KuduException
     */
    public static void delete(String table, KuduClient client, List<KuduRow> entitys) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_MULT);
            session.setMutationBufferSpace(BUFFER_SPACE);
            for (KuduRow entity : entitys) {
                Delete delete = kuduTable.newDelete();
                KuduUtil.operate(entity, delete, session);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行删除操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            //throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduUtil.close(session, client);
        }

    }

    /**
     * 单条删除 删除只能是主键
     *
     * @param table
     * @param client
     * @param entity
     * @throws KuduException
     */
    public static void delete(String table, KuduClient client, KuduRow entity) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_SINGLE);
            Delete delete = kuduTable.newDelete();
            OperationResponse operate = KuduUtil.operate(entity, delete, session);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行删除操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            //throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduUtil.close(session, client);
        }
    }
}
