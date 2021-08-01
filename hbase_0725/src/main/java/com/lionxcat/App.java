package com.lionxcat;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
public class App {
    private static final String _namespace = "baoxiao";
    private static final String _tablename = "baoxiao:student";
    private static final byte[] _cf_info = Bytes.toBytes("info");
    private static final byte[] _cf_score = Bytes.toBytes("score");
    private static final byte[] _cl_sid = Bytes.toBytes("student_id");
    private static final byte[] _cl_class = Bytes.toBytes("class");
    private static final byte[] _cl_under = Bytes.toBytes("understanding");
    private static final byte[] _cl_progr = Bytes.toBytes("programming");

    public static void main(String[] args) throws IOException {
        Connection conn = null;
        Table table = null;
        try {
            System.out.println("连接HBase...");
            // 获取HBase连接
            conn = getConnection();

            System.out.println("创建表...");
            // 创建表
            table = createTable(conn);

            System.out.println("添加数据...");
            // 添加列
            putColumn(table, "Tom", "20210000000001", 1);
            putColumn(table, "Jerry", "20210000000002", 1);
            putColumn(table, "Jack", "20210000000003", 2);
            putColumn(table, "Rose", "20210000000004", 2);
            putColumn(table, "BaoXiao", "20210607020479", 5);
            putColumn(table, "Tom", 75, 82);
            putColumn(table, "Jerry", 85, 67);
            putColumn(table, "Jack", 80, 80);
            putColumn(table, "Rose", 60, 61);
            putColumn(table, "BaoXiao", 66, 99);

            // 读取数据
            System.out.println("读取从A到ZZZ之间的数据...");
            printTable(table, "A", "ZZZ");

            System.out.println("Done!");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (table != null) {
                // 关闭表
                table.close();
            }
            if (conn != null) {
                // 关闭连接
                conn.close();
            }
        }
    }

    // 打印从from到to之间的数据
    private static void printTable(Table table, String from, String to) throws IOException {
        Scan scan = new Scan();
        // 设置读取起始位置
        if (!StringUtils.isEmpty(from)) {
            scan.withStartRow(Bytes.toBytes(from));
        }
        if (!StringUtils.isEmpty(to)) {
            scan.withStopRow(Bytes.toBytes(to));
        }
        // 设置客户端缓存行数
        scan.setCaching(10);
        for (Result r : table.getScanner(scan)) {
            for (Cell c : r.rawCells()) {
                System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "   Familiy:Quilifier : "
                        + Bytes.toString(CellUtil.cloneQualifier(c)) + "   Value : "
                        + Bytes.toString(CellUtil.cloneValue(c)) + "   Timestamp : " + c.getTimestamp());
            }
        }
    }

    // 添加info列族行数据
    private static void putColumn(Table table, String name, String student_id, int class_id) throws IOException {
        // 设置row key
        Put p = new Put(Bytes.toBytes(name));
        // 添加列到列族info
        p.addColumn(_cf_info, _cl_sid, Bytes.toBytes(student_id));
        p.addColumn(_cf_info, _cl_class, Bytes.toBytes(String.valueOf(class_id)));
        // 同步WAL
        p.setDurability(Durability.SYNC_WAL);
        table.put(p);
    }

    // 添加score列族行数据
    private static void putColumn(Table table, String name, int understanding, int programming) throws IOException {
        // 设置row key
        Put p = new Put(Bytes.toBytes(name));
        // 添加列到列族score
        p.addColumn(_cf_score, _cl_under, Bytes.toBytes(String.valueOf(understanding)));
        p.addColumn(_cf_score, _cl_progr, Bytes.toBytes(String.valueOf(programming)));
        // 同步WAL
        p.setDurability(Durability.SYNC_WAL);
        table.put(p);
    }

    // 创建表
    private static Table createTable(Connection conn)
            throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Admin admin = null;
        Table table = null;
        try {
            // 获得执行管理操作接口
            admin = conn.getAdmin();
            // 创建命名空间
            admin.createNamespace(NamespaceDescriptor.create(_namespace).build());
            // 设置表名
            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(_tablename));
            // 添加列族
            tableBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(_cf_info).build());
            tableBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(_cf_score).build());
            // 创建表
            admin.createTable(tableBuilder.build());
            // 获取新创建的表
            table = conn.getTable(TableName.valueOf(_tablename));
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (admin != null) {
                // 关闭
                admin.close();
            }
        }
        return table;
    }

    // 获取HBase连接器
    private static Connection getConnection()
            throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Configuration conf = HBaseConfiguration.create();
        // 隐藏真实ZK的IP，也可以用hbase-site.xml文件获取到地址
        conf.set("hbase.zookeeper.quorum", "bdc01,bdc02,bdc03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // Is HBase available? Throw an exception if not.
        HBaseAdmin.available(conf);
        Connection conn = ConnectionFactory.createConnection(conf);
        return conn;
    }
}
