package com.atlxl.ct.common.bean;

/**
 * 数据对象
 */
public abstract class Data implements Val{

    public String content;


    @Override
    public void setValue(Object val) {
        content = (String) val;
    }

    @Override
    public String getValue() {
        return content;
    }
}
