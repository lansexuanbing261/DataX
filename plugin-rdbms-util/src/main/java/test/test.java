package test;


import com.alibaba.datax.plugin.rdbms.util.DBUtil;

import java.sql.*;

public class test {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        test jt = new test();
        jt.select();
        //jt.insert();
        //jt.delete();
        //jt.update();

    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException{
        String path  = test.class.getClassLoader().getResource("test").getPath();
        System.out.println("path: " + path); ///home/wangxiaoliang/plugin-rdbms-util-0.0.1-SNAPSHOT.jar!/test
        String pathnew = System.getProperty("java.class.path"); //plugin-rdbms-util-0.0.1-SNAPSHOT.jar
        System.out.println("newpath: " + pathnew);

        String driver = "com.cloudera.impala.jdbc41.Driver";
        String url = "jdbc:impala://172.17.10.62:21050/k_yc_rpt;auth=noSasl";
        String username = "bigdata";
        String password = "bigdata";
        Connection conn = null;
        Class.forName(driver);
        conn = (Connection) DriverManager.getConnection(url,username,password);
        return conn;
    }

    public static void closeDBResources(ResultSet rs, PreparedStatement ps, Connection conn){
        if (rs != null){
            try {
                rs.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if (ps != null){
            try {
                ps.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if (conn != null){
            try {
                conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void closeDBResources(PreparedStatement ps,Connection conn){
        if (ps != null){
            try {
                ps.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if (conn != null){
            try {
                conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void select() throws ClassNotFoundException, SQLException {
        Connection conn = getConnection();
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {

            //String sql = "select * from k_yc_rpt.user_level where dt = 20181226 limit 10;";
            String sql = "select * from user_level where dt = 20181226 limit 10;";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            int col = rs.getMetaData().getColumnCount();
            System.out.println("=====================================");
            while (rs.next()){
                for(int i=1;i<=col;i++){
                    System.out.print(rs.getString(i)+"\t");
                }
                System.out.print("\n");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            closeDBResources(rs,ps,conn);
        }

        System.out.println("=====================================");
    }

    public void insert() throws ClassNotFoundException,SQLException{
        Connection conn = getConnection();
        Boolean rs = null;
        PreparedStatement ps = null;
        try {
            conn.setAutoCommit(false);
            String sql = "insert into table k_yc_rpt.user_level_test select * from k_yc_rpt.user_level where dt = 20181220;";
            System.out.println("sql: " + sql);
            ps = conn.prepareStatement(sql);
            //rs = ps.executeQuery();
            rs = ps.execute();
            conn.commit();
            //int col = rs.getMetaData().getColumnCount();
            System.out.println("=====================================");
            System.out.println("executor_result:" + rs);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            closeDBResources(ps,conn);
        }

        System.out.println("=====================================");

    }

    public void delete() throws ClassNotFoundException,SQLException{
        Connection conn = getConnection();
        Integer rs = null;
        PreparedStatement ps = null;
        try {
            conn.setAutoCommit(false);
            String sql = "delete from k_yc_rpt.user_level_test where dt = 20181127;";
            System.out.println("sql: " + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeUpdate();
            conn.commit();
            System.out.println("=====================================");
            System.out.println("executor_result:" + rs);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            closeDBResources(ps,conn);
        }
    }

    public void update() throws ClassNotFoundException,SQLException{
        Connection conn = getConnection();
        Integer rs = null;
        PreparedStatement ps = null;
        try {
            conn.setAutoCommit(false);
            String sql = "update  k_yc_rpt.user_level_test set device_id = 123456 where dt = 20181128 and user_id = 123;";
            System.out.println("sql: " + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeUpdate();
            conn.commit();
            System.out.println("=====================================");
            System.out.println("executor_result:" + rs);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            closeDBResources(ps,conn);
        }
    }
}
