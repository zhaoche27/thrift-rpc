package com.ttyc.datagroup.commons.util;

import junit.framework.TestCase;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class NetsTest extends TestCase {

    public void testLocalIp() throws Exception {
        System.out.println(Nets.getLocalIp());
    }
}