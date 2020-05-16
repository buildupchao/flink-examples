package com.buildupchao.flinkexamples.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author buildupchao
 * @date 2020/5/16 15:35
 * @since JDK1.8
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetricEvent {

    /**
     * metric name
     */
    private String name;

    /**
     * metric timestamp
     */
    private Long timestamp;

    /**
     * metric fields
     */
    private Map<String, Object> fields;

    /**
     * metric tags
     */
    private Map<String, String> tags;
}
