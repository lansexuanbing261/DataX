package com.alibaba.datax.plugin.writer.kuduwriter.util;

import org.apache.kudu.Type;

public class KuduColumn {
    private Integer index;
    private Object rawData;
    private Type type;
    private boolean isSelect;
    private boolean isUpdate;
    //private boolean isPrimaryKey = false;


    public Integer getIndex(){return index;}
    public KuduColumn setIndex(Integer index){  this.index = index; return this;}

    public Object getRawData(){ return rawData;}
    public KuduColumn setRawData(Object rawData){   this.rawData = rawData; return this;}

    public Type getType(){ return type;}
    public KuduColumn setType(Type type){   this.type = type;return this;}

    public boolean isSelect() {
        return isSelect;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public KuduColumn setUpdate(boolean update) {
        isUpdate = update;
        return this;
    }

//    public boolean isPrimaryKey() {
//        return isPrimaryKey;
//    }
//
//    public KuduColumn setPrimaryKey(boolean primaryKey) {
//        isPrimaryKey = primaryKey;
//        return this;
//    }

    public String toString() {
        return "KuduColumn{" +
                "columnIndex='" + index + '\'' +
                ", columnType=" + type +
                ", columnValue=" + rawData +
                ", isUpdate=" + isUpdate +
               // ", isPrimaryKey=" + isPrimaryKey +
               // ", alterColumnEnum=" + alterColumnEnum +
//                ", defaultValue=" + defaultValue +
//                ", isNullAble=" + isNullAble +
//                ", newColumnName='" + newColumnName + '\'' +
//                ", comparisonOp=" + comparisonOp +
//                ", comparisonValue=" + comparisonValue +
                ", isSelect=" + isSelect +
                '}';
    }
}
