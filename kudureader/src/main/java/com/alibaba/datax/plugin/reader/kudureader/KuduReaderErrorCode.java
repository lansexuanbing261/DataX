package com.alibaba.datax.plugin.reader.kudureader;
import com.alibaba.datax.common.spi.ErrorCode;

public enum KuduReaderErrorCode implements ErrorCode {

    REQUIRED_VALUE("KuduReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("KuduReader-01", "您配置的值不合法."),
    UNEXCEPT_EXCEPTION("UNEXCEPT_EXCEPTION","未知异常");;


    private final String code;
    private final String description;

    private KuduReaderErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }

}
