package com.buildupchao.flinkexamples.connector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author buildupchao
 * @date 2020/5/16 16:52
 * @since JDK1.8
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Student {
    private int id;
    private String name;
    private String password;
    private int age;
}
