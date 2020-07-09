package com.company.utils;

import java.sql.*;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * 1. 数据仓库连接
 * 2. 数据库列表预览
 * 3. 数据表列表预览
 * 4. 数据表结构预览
 * 5. 样例数据预览(抽样)
 * 6. 切换数据库
 */

public class HiveUtil {
  private Connection conn = null;
  PreparedStatement preparedStatement = null;
  ResultSet resultSet = null;
  Statement statement = null;

  static {
    try {
      //1、加载驱动
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  //构造方法：打开一个连接
  public HiveUtil() {
    try {
      //1、数据仓库连接
      conn = DriverManager.getConnection("jdbc:hive2://bgnode3:10010", "hive", "");
      statement = conn.createStatement();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * 2. 数据库列表预览
   *
   * @return 返回数据库名称集合
   * 语句：show databases
   */
  public List<String> getDatabases() {
    List result = new ArrayList<String>();

    //执行查询
    try {
      preparedStatement = conn.prepareStatement("show databases");
      resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
      return result;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * 3、数据库列表预览
   * show tables
   *
   * @param database 要获取哪个数据库中的表
   * @return 返回的是数据表的集合
   * 语句：show {database}.{table}
   */
  public List<String> getTables(String database) {
    List result = new ArrayList<String>();

    //执行查询
    try {
      //切换数据库
      changeDatabase(database);
      resultSet = statement.executeQuery("show tables");
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
      return result;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * 4、 数据表结构预览
   *
   * @param database 数据库名称
   * @param table    数据表名称
   * @return 返回的是表的描述性息的集合
   * 语句：desc {database}.{table}
   */
  //需要参数：数据库名，表名
  public List<String> getTableDesc(String database, String table) {
    List<String> result = new ArrayList<>();

    try {
      //创建preparedStatement
      preparedStatement = conn.prepareStatement("desc " + database + "." + table);
      //执行查询操作
      ResultSet resultSet = preparedStatement.executeQuery();

      //将结果封装到list中
      while (resultSet.next()) {
        result.add(resultSet.getString(1) + "  " + resultSet.getString(2));
      }

      return result;

    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 5. 样例数据预览(抽样)——>limit实现
   *
   * @param database 数据库名称
   * @param table    数据表名称
   * @param n        获取前n行记录
   *                 语句：select * from {database}.{table} limit {number}
   */
  public List<String> getDataByLimit(String database, String table, int n) {
    int columns = getTableDesc(database, table).size();
    List<String> list = new ArrayList<>();
    try {
      preparedStatement = conn.prepareStatement("select * from " + database + "." + table + " limit " + n);
      resultSet = preparedStatement.executeQuery();
      return getData(resultSet, columns);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 5. 样例数据预览(抽样)——> tablesample实现
   *
   * @param database 数据库名称
   * @param table    数据表
   * @param percent  抽样比例
   *                 语句: select * from {database}.{table} tablesample({percent} percent)
   */
  public List<String> getDataBySample(String database, String table, double percent) {
    int columns = getTableDesc(database, table).size();
    List<String> list = new ArrayList<>();
    try {
      preparedStatement = conn.prepareStatement("select * from " + database + "." + table + " tablesample(" + percent + " percent)");
      resultSet = preparedStatement.executeQuery();
      return getData(resultSet, columns);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 5. 样例数据预览(抽样)——> rand实现
   *
   * @param database 数据库名称
   * @param table    数据表
   * @param n        随机取出前n行记录
   *                 语句：select *,rand()r from {database}.{table} order by r limit n
   */
  public List<String> getDataByRandom(String database, String table, int n) {
    int columns = getTableDesc(database, table).size();
    List<String> list = new ArrayList<>();
    try {
      preparedStatement = conn.prepareStatement("select *,rand() r from " + database + "." + table + " order by r limit " + n);
      resultSet = preparedStatement.executeQuery();
      return getData(resultSet, columns - 1);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 数据封装工具
   *
   * @param resultSet 结果集
   * @param size      数据列数量,也就是要获取几列的数据
   * @return 封装的List数据集
   */
  private List<String> getData(ResultSet resultSet, int size) {
    try {
      List<String> list = new ArrayList<>();
      while (resultSet.next()) {
        String line = "";
        for (int i = 0; i < size; i++) {
          line += resultSet.getString(i + 1) + "\t";
        }
        line = line.substring(0, line.length() - 1);        //左闭右开
        //将每一条记录添加到lisi中
        list.add(line);
      }
      return list;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * 通过自定义的SQL返回结果
   *
   * @param sql 要执行的sql语句——包含数据库名称
   * @return 执行SQL的结果集
   * <p>
   * 前期准备：hive中存在数据库temp，用于存放临时SQL去查询的中间表
   * ① 将查询到的结果集生成中间表存放到temp数据库下 create table {table} as sql
   * ② 再通过查询此中间表返回结果集合
   * ③ 表名随机：tmp_userId_时间戳
   */
  public List<String> getDataBySQL(String sql) {
    //将表名进行随机：tmp_userId_时间戳
    //时间戳——> long ——> 1970-01-01 00:00:00 ——> Unix元年

    try {
      String tableName = "temp_1_" + new Date().getTime();
      preparedStatement = conn.prepareStatement("create table temp." + tableName + " as " + sql);
      preparedStatement.execute();

      //获取表的字段个数
      int size = getTableDesc("temp", tableName).size();
      preparedStatement = conn.prepareStatement("select * from temp." + tableName);
      ResultSet resultSet = preparedStatement.executeQuery();
      return getData(resultSet, size);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }


  /**
   * 从数据表中查询指定列的数据
   *
   * @param sql          要执行的sql语句——>包含数据库名称
   * @param columnsIndex 待查询的列的索引
   * @return SQL执行的结果集
   */
  public List<String> getDataBySQLWithColumns(String sql, int[] columnsIndex) {
    try {
      String tableName = "tmp_1_" + new Date().getTime();
      preparedStatement = conn.prepareStatement("create table temp." + tableName + " as " + sql);
      resultSet = preparedStatement.executeQuery();
      return getDataByColumns(resultSet, columnsIndex);

    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * @param resultSet   查询结果集
   * @param columnIndex 数据列索引
   * @return 封装的List数据集
   */
  private List<String> getDataByColumns(ResultSet resultSet, int[] columnIndex) {
    List<String> list = new ArrayList<>();

    try {
      while (resultSet.next()) {
        String line = "";

        for (int i = 0; i < columnIndex.length; i++) {
          line += resultSet.getString(columnIndex[i]) + ",";
        }
        line = line.substring(0, line.length() - 1);
        list.add(line);
      }
      return list;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * 执行查询语句，返回结果
   *
   * @param sql   需要执行的sql语句
   * @param paras 执行查询时所需要的参数
   * @return 结果集
   */
  public ResultSet executeQuery(String sql, Object[] paras) {
    try {
      preparedStatement = conn.prepareStatement(sql);
      getPreparedStatement(paras);
      return preparedStatement.executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * 将传入的参数进行赋值，构建一个完整的PreparedStatement
   *
   * @param paras 传入的参数
   */
  private void getPreparedStatement(Object[] paras) {
    try {
      if (paras != null) {
        for (int i = 0; i < paras.length; i++) {
          preparedStatement.setObject(i + 1, paras[i]);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 执行一个不需要返回结果的命令
   *
   * @param sql   需要执行的sql
   * @param paras 执行sql需要的参数
   */
  public void execute(String sql, Object[] paras) {
    try {
      preparedStatement = conn.prepareStatement(sql);
      getPreparedStatement(paras);
      preparedStatement.execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * 6、切换数据库
   *
   * @param database 数据库名称
   *                 语句：use {database}
   */
  public void changeDatabase(String database) {
    try {
      statement.execute("use " + database);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * 释放资源
   */
  public void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      if (conn != null) {
        conn.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}