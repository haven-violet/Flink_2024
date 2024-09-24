package cn.doitedu.flink.java.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaoBaoBean {
    private String name;
    private int timstamp;
    private int wasteMoney;
}
