package cn.pan.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Resource;

@Resource
@Data
@NoArgsConstructor
public class UserDoc {
    private String title;
    private String filecontent;

    public UserDoc(String title, String filecontent) {
        this.title = title;
        this.filecontent = filecontent;
    }
}
