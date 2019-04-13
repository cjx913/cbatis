package com.cjx913.cbatis.core;

public class CbatisUtil {
    public static String split(String[] strings){
        if (strings==null){
            return null;
        }
        StringBuilder stringBuilder =new StringBuilder();
        for(String str:strings){
            stringBuilder.append(str+",");
        }
        return stringBuilder.deleteCharAt(stringBuilder.length() - 1).toString();
    }
}
