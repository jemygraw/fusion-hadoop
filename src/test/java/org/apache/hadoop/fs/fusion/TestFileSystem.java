package org.apache.hadoop.fs.fusion;

import org.junit.Test;

/**
 * Created by jemy on 04/05/2017.
 */
public class TestFileSystem {
    @Test
    public void testFileSystem() {
        String str = "2017-05-21-23";

        String itemDay = str.substring(0, 10);
        String itemHour = str.substring(11, 13);
        System.out.println(itemDay);
        System.out.println(itemHour);
    }
}
