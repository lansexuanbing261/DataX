package com.alibaba.datax.plugin.reader.kudureader;


import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;

import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KuduReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Impala;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(com.yongche.datax.plugin.reader.kudureader.KuduReader.Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

//            Integer userConfigedFetchSize = this.originalConfig.getInt(Constant.DEFAULT_FETCH_SIZE);
//            if (userConfigedFetchSize != null) {
//                LOG.warn("对 kudureader 不需要配置 fetchSize, kudureader 将会忽略这项配置. 如果您不想再看到此警告,请去除fetchSize 配置.");
//            }

            //this.originalConfig.set(Constant.DEFAULT_FETCH_SIZE, Integer.MIN_VALUE);

            dealFetchSize(this.originalConfig);
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck(){
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }


        private void dealFetchSize(Configuration originalConfig) {
            int fetchSize = originalConfig.getInt(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    Constant.DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw DataXException
                        .asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                                String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
                                        fetchSize));
            }
            originalConfig.set(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    fetchSize);
        }
    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE,super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }
}
