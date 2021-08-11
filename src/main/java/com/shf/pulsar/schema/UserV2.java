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
 * @date 2021/8/10 10:49
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserV2 {

    private int id;
    private String name;
    private int age;
    private String sex;

}
