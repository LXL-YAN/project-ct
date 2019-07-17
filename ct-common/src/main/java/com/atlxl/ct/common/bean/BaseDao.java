package com.atlxl.ct.common.bean;

import com.atlxl.ct.common.constant.Names;
import com.atlxl.ct.common.constant.ValueConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 基础的数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws Exception {
        getConnection();
        getAdmin();
    }

    protected void end() throws Exception {
        Admin admin = getAdmin();
        if ( admin != null ) {
            admin.close();
            adminHolder.remove();
        }

        Connection conn = getConnection();
        if ( conn != null ) {
            conn.close();
            connHolder.remove();
        }
    }

    /**
     * 创建表,如果表已经存在,那么删除后再创建新的
     * @param name
     * @param families
     */
    protected void createTableXX( String name, String... families ) throws Exception {
        createTableXX(name, null, families);
    }

    protected void createTableXX( String name, Integer regionCount, String... families ) throws Exception {
        Admin admin = getAdmin();

        TableName tableName = TableName.valueOf(name);

        if ( admin.tableExists(tableName) ) {
            // 表存在,删除表
            deleteTable(name);
        }

        // 创建表
        createTable(name, regionCount, families);
    }

    /**
     * 创建表
     * @param name
     * @param families
     * @throws Exception
     */
    private void createTable( String name, Integer regionCount, String... families ) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        if ( families == null || families.length == 0 ) {
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }

        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }

        // 增加预分区
        if ( regionCount == null || regionCount <= 1 ) {
            admin.createTable(tableDescriptor);
        } else {
            // 分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }
    }

    /**
     * 计算分区号
     * @param tel
     * @param date
     * @return
     */
    protected int genRegionNum( String tel, String date ) {

        // 13301234567
        String usercode = tel.substring(tel.length() - 4);
        // 20181003100000
        String yearMonth = date.substring(0, 6);

        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        // crc校验采用异或算法, hash
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

//    public static void main(String[] args) {
//        System.out.println(genRegionNum("13309090909", "20181003100000"));
//    }

    /**
     * 生成分区键
     */
    private byte[][] genSplitKeys(int regionCount) {
        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        // 0|, 1|, 2|, 3|, 4|
        // (-∞, 0|), [0|, 1|), [1|, +∞)
        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }

        bsList.toArray(bs);

        return bs;
    }

    /**
     * 增加数据
     * @param name
     * @param put
     * @throws Exception
     */
    protected void putData( String name, Put put) throws Exception {

        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 删除表
     * @param name
     * @throws Exception
     */
    protected void deleteTable(String name) throws Exception {
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 创建命名空间,如果命名空间已经存在就不需要创建,否则创建新的.
     * @param namespace
     */
    protected  void createNamespaceNX( String namespace ) throws Exception {
        Admin admin = getAdmin();

        try {
            admin.getNamespaceDescriptor(namespace);
        } catch ( NamespaceNotFoundException e ) {
            // e.printStackTrace();

            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 获取连接对象
     */
    protected synchronized Admin getAdmin() throws Exception {
        Admin admin = adminHolder.get();
        if ( admin == null ) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 获取连接对象
     */
    protected synchronized Connection getConnection() throws Exception {
        Connection conn = connHolder.get();
        if ( conn == null ) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }
}
