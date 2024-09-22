package cn.doitedu.flink.java.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventBean2 {
    private long guid;
    private String eventId;
    private long timestamp;
    private String pageId;
    private int actTimeLog; // 行为时长
}
