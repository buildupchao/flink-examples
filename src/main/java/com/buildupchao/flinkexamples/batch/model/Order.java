package com.buildupchao.flinkexamples.batch.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * @author buildupchao
 * @date 2020/01/13 23:50
 * @since JDK 1.8
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order implements Serializable {

    private Integer orderId;
    private String orderNo;
    private Integer userId;
    private Integer goodId;
    private BigDecimal goodsMoney;
    private BigDecimal realTotalMoney;
    private Integer payFrom;
    private String province;
    private Timestamp createTime;
}
