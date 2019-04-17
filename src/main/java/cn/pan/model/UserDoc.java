package cn.pan.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Resource;

@Resource
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserDoc {
    private String title;
    private String filecontent;
}
