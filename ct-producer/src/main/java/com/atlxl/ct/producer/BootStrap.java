package com.atlxl.ct.producer;

import com.atlxl.ct.common.bean.Producer;
import com.atlxl.ct.producer.bean.LocalFileProducer;
import com.atlxl.ct.producer.io.LocalFileDataIn;
import com.atlxl.ct.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * 启动对象
 */
public class BootStrap {
    public static void main(String[] args) throws Exception {

//        if ( args.length < 2 ) {
//            System.out.println("系统参数错误！清按照指定的格式转递：java -jar Produce.jar path1 path2 ");
//            System.exit(1);
//        }

        //构建生成者对象
        Producer producer = new LocalFileProducer();

        producer.setIn(new LocalFileDataIn("D:\\Data\\项目\\大数据电信客服项目\\2.资料\\辅助文档\\contact.log"));
        producer.setOut(new LocalFileDataOut("D:\\Data\\项目\\大数据电信客服项目\\2.资料\\辅助文档\\call.log"));

//        producer.setIn(new LocalFileDataIn(args[0]));
//        producer.setOut(new LocalFileDataOut(args[1]));

        //生产数据
        producer.produce();

        //关闭生产者对象
        producer.close();

    }
}
