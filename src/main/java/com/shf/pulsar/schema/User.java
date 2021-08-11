package com.shf.pulsar.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/7 22:39
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class User {

    private int id;
    private String name;
    private int age;

}
