package com.huangjunyi1993.zeromq.core.writer;

import java.lang.annotation.*;

/**
 * 写入策略注解
 * Created by huangjunyi on 2022/12/11.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface WriteStrategy {
    String value();
}
