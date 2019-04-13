package com.cjx913.cbatis.core;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

@Intercepts({
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}),
})
public class SubTableInterceptor implements Interceptor {

    private SubTableHandler subTableHandler;
    private Set <String> handleSqlIds;

    public Object intercept(Invocation invocation) throws Throwable {
        Executor executor = (Executor) invocation.getTarget();

        Object[] args = invocation.getArgs();
        MappedStatement mappedStatement = (MappedStatement) args[0];
        Object parameter = args[1];

        String sqlId = mappedStatement.getId();
        //是否处理
        if (handleSqlIds == null || handleSqlIds.isEmpty() || !handleSqlIds.contains(sqlId)) {
            return invocation.proceed();
        }


        if (mappedStatement.getSqlCommandType().compareTo(SqlCommandType.SELECT) == 0) {
            RowBounds rowBounds = (RowBounds) args[2];
            ResultHandler resultHandler = (ResultHandler) args[3];

            CacheKey cacheKey;
            BoundSql boundSql;
            if (args.length == 4) {
                boundSql = mappedStatement.getBoundSql(parameter);
                cacheKey = executor.createCacheKey(mappedStatement, parameter, rowBounds, boundSql);
            } else {
                boundSql = (BoundSql) args[5];
                cacheKey = (CacheKey) args[4];
            }

            subTableHandler.boundSqlHandler(boundSql);

            return executor.query(mappedStatement, parameter, rowBounds, resultHandler, cacheKey, boundSql);
        } else {
            MappedStatement newMappedStatement = subTableHandler.mappedStatementHandler(mappedStatement, parameter);
            return executor.update(newMappedStatement, parameter);
        }
    }

    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    public void setProperties(Properties properties) {
        String tableNameHandlerClassName = properties.getProperty("table-name-handler").trim();
        if (tableNameHandlerClassName == null || "".equalsIgnoreCase(tableNameHandlerClassName)) {
            throw new CbatisException("无效的名表处理类！请在插件添加<property name=\"table-name-tableNameHandler\" value=\"表明处理类完全限定名\"/>\n");
        }
        subTableHandler = new SubTableHandler(tableNameHandlerClassName);

        String[] sqlIds = properties.getProperty("handle-sql-ids").split(",");
        if (sqlIds != null && sqlIds.length > 0) {
            handleSqlIds = new HashSet <>();
            for (String s : sqlIds) {
                handleSqlIds.add(s.trim());
            }
        }
    }
}
