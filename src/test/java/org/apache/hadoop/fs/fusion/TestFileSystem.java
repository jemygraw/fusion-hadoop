package org.apache.hadoop.fs.fusion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Created by jemy on 04/05/2017.
 */
public class TestFileSystem {
    @Test
    public void testFileSystem() throws IOException {
        String authority = "DOMAIN";
        String day = "2017-05-22";

        Configuration cfg = new Configuration();
        cfg.set("fs.qiniu.access.key", "ACCESS KEY");
        cfg.set("fs.qiniu.secret.key", "SECRET KEY");

        URI uri = URI.create(String.format("fusion://%s/%s/", authority, day));
        FusionFileSystem fs = new FusionFileSystem();
        fs.initialize(uri, cfg);
        FileStatus fileStatus = fs.getFileStatus(new Path(uri));
        Assert.assertNotNull(fileStatus);
        System.out.println(fileStatus.getPath());
        FileStatus[] fileStatuses = fs.listStatus(new Path(uri));
        Assert.assertNotNull(fileStatuses);
        for (FileStatus status : fileStatuses) {
            System.out.println(status.getPath());
        }
    }
}
