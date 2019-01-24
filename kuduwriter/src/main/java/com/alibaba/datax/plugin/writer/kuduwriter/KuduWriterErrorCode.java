package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KuduWriterErrorCode implements ErrorCode {

    CONFIG_INVALID_EXCEPTION("KuduWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("KuduWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("KuduWriter-02", "您填写的参数值不合法."),
    GET_KUDU_CONNECTION_ERROR("KuduWriter-03", "您配置的参数无法连接master."),
    CLOSE_KUDU_CLIENT_ERROR("KuduWriter-04", "关闭kudu 客户端的时候出现异常."),
    WRITER_RUNTIME_EXCEPTION("KuduWriter-05", "出现运行时异常, 请联系我们."),
    TRUNCATE_KUDU_TABLE_ERROR("KuduWriter-06","清空kudu失败，请查看日志");

    private final String code;
    private final String description;

    private KuduWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
