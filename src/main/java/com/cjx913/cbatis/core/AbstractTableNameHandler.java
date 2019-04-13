package com.cjx913.cbatis.core;

import org.apache.ibatis.binding.MapperMethod;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * 表名处理器
 */
public abstract class AbstractTableNameHandler {
    private Map <String, TableNameHandlerMethod> tableMethodMap = new HashMap <>();

    public AbstractTableNameHandler() {
        initTableMethodMap();
    }

    public final String tableNameHandler(String tableName, Object parameter) {

        TableNameHandlerMethod tableNameHandlerMethod = tableMethodMap.get(tableName.toUpperCase());
        if (tableNameHandlerMethod == null) {
            return tableName.trim().toUpperCase();
        }
        Method method = tableNameHandlerMethod.getMethod();

        //获取方法参数值，可以为null
        Object[] params = paramHandler(method, parameter);
//        if (params == null) {
//            throw new CbatisException("调用分表处理方法错误！没有指定参数\n表名:" + tableName + ",方法:" + method.getName());
//        }

        Object newTableName = null;
        try {
            newTableName = method.invoke(tableNameHandlerMethod.getInstanse(), params);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new CbatisException("调用分表处理方法错误！表名:" + tableName + ",方法:" + method.getName(), e);
        }
        if (newTableName instanceof String) {
            return ((String) newTableName).trim().toUpperCase();
        } else {
            throw new CbatisException("调用分表处理方法错误！->返回非字符串。表名:" + tableName);
        }
    }

    /**
     * 获取参数值
     *
     * @param method
     * @param parameter
     * @return
     */
    private Object[] paramHandler(Method method, Object parameter) {

        MapperMethod.ParamMap <Object> paramMap = null;
        if (parameter instanceof MapperMethod.ParamMap) {
            paramMap = (MapperMethod.ParamMap <Object>) parameter;
        }

        List <String> params = new ArrayList <>();
        Annotation parameterAnnotations[][] = method.getParameterAnnotations();
        for (int i = 0; i < parameterAnnotations.length; i++) {
            for (Annotation annotation : parameterAnnotations[i]) {
                if (Param.class.equals(annotation.annotationType())) {
                    params.add(((Param) annotation).value());
                }
            }
        }
        //获取参数值
        List <Object> parameters = new ArrayList <>();
        try {
            if (paramMap != null) {
                for (String param : params) {
                    Object p = paramMap.get(param);
                    parameters.add(p);
                }
            } else {
                for (String param : params) {
                    PropertyDescriptor pd = new PropertyDescriptor(param, parameter.getClass());
                    Method getMethod = pd.getReadMethod();
                    Object p = getMethod.invoke(parameter);
                    parameters.add(p);
                }
            }
        } catch (IllegalAccessException | InvocationTargetException | IntrospectionException e) {
            throw new CbatisException("获取参数值错误", e);
        }

//        if (parameters.isEmpty()) {
//            return null;
//        } else {
//            return parameters.toArray();
//        }
        return parameters.toArray();
    }

    /**
     * @return 表名作为键，处理该表的方法作为值。<br/>
     * table name as key ,tableNameHandler method as value.
     */
    private void initTableMethodMap() {
        try {
            for (TableNameHandlerMethod tableNameHandlerMethod : setTableMethodSet()) {
                this.tableMethodMap.put(tableNameHandlerMethod.getMethod().getName().toUpperCase(), tableNameHandlerMethod);
            }
        } catch (NoSuchMethodException e) {
            throw new CbatisException("调用参数处理方法错误！", e);
        }
    }

    /**
     * 表名处理方法
     *
     * @return
     * @throws NoSuchMethodException
     */
    public abstract Set <TableNameHandlerMethod> setTableMethodSet() throws NoSuchMethodException;

    protected TableNameHandlerMethod getThisTableNameHandlerMethod(Method method) {
        return new TableNameHandlerMethod(this, method);
    }


    public class TableNameHandlerMethod {
        private Object instanse;
        private Method method;

        /**
         * 限制在AbstractTableNameHandler子类写表名处理方法
         *
         * @param method
         */
        public TableNameHandlerMethod(Method method) {
            instanse = AbstractTableNameHandler.this;
            this.method = method;
        }

        private TableNameHandlerMethod(Object instanse, Method method) {
            this.instanse = instanse;
            this.method = method;
        }

        public Object getInstanse() {
            return instanse;
        }

        public void setInstanse(Object instanse) {
            this.instanse = instanse;
        }

        public Method getMethod() {
            return method;
        }

        public void setMethod(Method method) {
            this.method = method;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TableNameHandlerMethod that = (TableNameHandlerMethod) o;

            return method.getName().equalsIgnoreCase(that.method.getName());
        }

        @Override
        public int hashCode() {
            return method.getName().toUpperCase().hashCode();
        }
    }
}
