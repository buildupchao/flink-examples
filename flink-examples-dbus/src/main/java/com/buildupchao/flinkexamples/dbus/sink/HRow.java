package com.buildupchao.flinkexamples.dbus.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase操作对象类
 *
 * @author buildupchao
 * @date 2020/02/04 16:08
 * @since JDK 1.8
 */
@Data
public class HRow implements Serializable {

    private static final long serialVersionUID = -3688365628306429297L;

    private byte[] rowKey;
    private List<HCell> cells = new ArrayList<>();

    public HRow() {
    }

    public HRow(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public void addCell(String family, String qualifier, byte[] value) {
        HCell hCell = new HCell(family, qualifier, value);
        cells.add(hCell);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HCell {
        private String family;
        private String qualifier;
        private byte[] value;

    }
}
