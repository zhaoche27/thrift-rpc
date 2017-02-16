package com.ttyc.datagroup.commons.zookeeper;

import java.util.List;

/**
 * Created by zhaoche on 2017/2/4.
 */
public interface ChildListener {

    void childChanged(String path, List<String> children);
}
