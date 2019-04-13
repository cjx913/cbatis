package com.cjx913.cbatis.core;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

import java.lang.reflect.Field;
import java.util.*;

/**
 * 分表处理器
 */
public class SubTableHandler {
    private AbstractTableNameHandler tableNameHandler;

    public SubTableHandler(String tableNameHandlerClassName) {
        try {
            tableNameHandler = (AbstractTableNameHandler) Class.forName(tableNameHandlerClassName).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new CbatisException("分表处理器错误->表名处理类错误！请确认" + tableNameHandlerClassName + "是否存在", e);
        }
    }

    public MappedStatement mappedStatementHandler(MappedStatement mappedStatement, Object parameter) {
        SqlSource sqlSource = mappedStatement.getSqlSource();

        BoundSql boundSql = sqlSource.getBoundSql(parameter);
        boundSqlHandler(boundSql);

        StaticSqlSource newSqlSource = new StaticSqlSource(mappedStatement.getConfiguration(), boundSql.getSql(), boundSql.getParameterMappings());

        MappedStatement newMappedStatement = new MappedStatement.Builder(mappedStatement.getConfiguration(), mappedStatement.getId(), newSqlSource, mappedStatement.getSqlCommandType())
                .resource(mappedStatement.getResource())
                .parameterMap(mappedStatement.getParameterMap())
                .flushCacheRequired(mappedStatement.isFlushCacheRequired())
                .keyGenerator(mappedStatement.getKeyGenerator())
                .keyProperty(CbatisUtil.split(mappedStatement.getKeyProperties()))
                .keyColumn(CbatisUtil.split(mappedStatement.getKeyColumns()))
                .timeout(mappedStatement.getTimeout())
                .cache(mappedStatement.getCache())
                .useCache(mappedStatement.isUseCache())
                .databaseId(mappedStatement.getDatabaseId())
                .fetchSize(mappedStatement.getFetchSize())
                .lang(mappedStatement.getLang())
                .resultMaps(mappedStatement.getResultMaps())
                .resultOrdered(mappedStatement.isResultOrdered())
                .build();
        return newMappedStatement;
    }


    public String boundSqlHandler(BoundSql boundSql) {
        try {
            String sql = boundSql.getSql();
            Object parameter = boundSql.getParameterObject();

            sql = sqlHandler(sql);

            Set <String> tableNames = getTableNames(sql);
            if (!tableNames.isEmpty()) {
                Map <String, String> newTableNames = getNewTableNames(tableNames, parameter);
                if (!newTableNames.isEmpty()) {
                    sql = replaceTableName(sql, newTableNames);
                    //通过反射修改sql语句
                    Field field = boundSql.getClass().getDeclaredField("sql");
                    field.setAccessible(true);
                    field.set(boundSql, sql);
                }
            }
            return sql;
        } catch (Exception e) {
            throw new CbatisException("Sql修改错误！", e);
        }
    }

    /**
     * sql处理，去掉换行和多余空格
     *
     * @param sql
     * @return
     */
    private String sqlHandler(String sql) {
        String[] strings = sql.trim().replace("\n", " ").split("\\s+");
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            if (strings[i].endsWith(",")) {
                stringBuilder.append(strings[i]);
            } else {
                stringBuilder.append(strings[i] + " ");
            }
        }
        sql = stringBuilder.toString().trim().toUpperCase();
        return sql;
    }

    /**
     * @param sql
     * @return 获取sql所有的表名
     */
    private Set <String> getTableNames(String sql) {
        Statement statement = null;
        try {
            statement = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            throw new CbatisException("解析sql语句错误！sql:" + sql, e);
        }
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List <String> tableList = tablesNamesFinder.getTableList(statement);
        Set <String> tableNames = new HashSet <>();
        for (String tableName : tableList) {
            //获取去掉“`”的表名
            if (tableName.startsWith("`") && tableName.endsWith("`")) {
                tableNames.add(tableName.substring(1, tableName.length()-1));
            }else {
                tableNames.add(tableName);
            }
        }

        return tableNames;
    }

    /**
     * @param tableNames
     * @param parameter
     * @return 原表名与新表名的Map
     */
    private Map <String, String> getNewTableNames(Set <String> tableNames, Object parameter) {
        Map <String, String> newTableNames = new HashMap <>();

        for (String tableName : tableNames) {
            //获取新表名（新表名都带“`”）
            String newTableName = "`"+tableNameHandler.tableNameHandler(tableName, parameter)+"`";
            newTableNames.put(tableName, newTableName);
        }
        return newTableNames;
    }

    /**
     * 表名替换
     *
     * @param sql
     * @param tableNames
     * @return
     */
    private String replaceTableName(String sql, Map <String, String> tableNames) {
        //去掉sq的“`”
        sql = sql.replace("`", "");
        Set <Map.Entry <String, String>> entrySet = tableNames.entrySet();
        for (Map.Entry <String, String> entry : entrySet) {
            if (!entry.getKey().equalsIgnoreCase(entry.getValue())) {
                sql = sql.replace(" " + entry.getKey() + " ", " " + entry.getValue() + " ")
                        .replace(" " + entry.getKey() + ",", " " + entry.getValue() + ",")
                        .replace("," + entry.getKey() + " ", "," + entry.getValue() + " ")
                        .replace("," + entry.getKey() + ",", "," + entry.getValue() + ",")
                        .replace(" " + entry.getKey() + "(", " " + entry.getValue() + "(");
            }
        }
        return sql.toUpperCase();
    }
}
