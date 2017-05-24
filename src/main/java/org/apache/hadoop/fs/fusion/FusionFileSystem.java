package org.apache.hadoop.fs.fusion;

import com.qiniu.common.QiniuException;
import okhttp3.OkHttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jemy on 27/04/2017.
 */
public class FusionFileSystem extends FileSystem {
    private Logger log = Logger.getLogger(FusionFileSystem.class);
    private static final String FUSION_VERSION = "v2";
    private static final String FUSION_FS_ACCESS_KEY = "fs.qiniu.access.key";
    private static final String FUSION_FS_SECRET_KEY = "fs.qiniu.secret.key";
    private OkHttpClient fsClient;
    private Path workingDir;
    private URI baseUri;
    private String logDomain;
    private String logDay;
    private String logHour;
    private String logFile;
    private String accessKey;
    private String secretKey;
    private FusionLogger fusionLogger;
    private FusionLogger.LogItem[] fusionLogItems;

    /**
     * fusion://domain/2017-04-30/00/part-00000.gz
     * <p>
     * v2/img.yimutian.com_2017-04-30-00_part-00000.gz
     */
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        this.baseUri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/").makeQualified(this.baseUri,
                this.getWorkingDirectory());

        log.info("base uri is " + this.baseUri.toString());
        log.info("working dir is " + this.workingDir.toString());
        this.logDomain = uri.getAuthority();
        log.info("parse hdfs uri " + uri.toString());
        String[] logItems = uri.getPath().split("/");
        switch (logItems.length) {
            case 4:
                this.logDay = logItems[1]; // 2017-04-30
                this.logHour = logItems[2]; // 00
                this.logFile = logItems[3]; // part-00000.gz
                break;
            case 3:
                this.logDay = logItems[1];
                this.logHour = logItems[2];
                break;
            case 2:
                this.logDay = logItems[1];
                break;
        }

        this.accessKey = conf.getTrimmed(FUSION_FS_ACCESS_KEY);
        this.secretKey = conf.getTrimmed(FUSION_FS_SECRET_KEY);
        this.fusionLogger = new FusionLogger(this.accessKey, this.secretKey);
        log.info("get log list for domain:" + this.logDomain + ", day: " + this.logDay);
        try {
            FusionLogger.LogListResult logListResult = this.fusionLogger.getLogList(new String[]{this.logDomain},
                    logDay);
            this.fusionLogItems = logListResult.data.get(this.logDomain);
        } catch (QiniuException ex) {
            log.error("get log list error, " + ex.response);
            throw ex;
        }

        this.setConf(conf);
        this.fsClient = new OkHttpClient();
    }

    public URI getUri() {
        return this.baseUri;
    }

    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        log.info("open fs data input stream for " + path.toString());
        String[] fileItems = path.toUri().getPath().split("/");
        String domain = path.toUri().getAuthority();
        String itemDay = fileItems[1];
        String itemHour = fileItems[2];
        String itemPartName = fileItems[3];
        String fusionPath = String.format("%s/%s_%s-%s_%s", FUSION_VERSION, domain, itemDay, itemHour, itemPartName);
        log.info("find input stream for log file " + fusionPath);
        for (FusionLogger.LogItem item : this.fusionLogItems) {
            if (item.name.equals(fusionPath)) {
                return new FSDataInputStream(new FusionInputStream(this.fsClient, item));
            }
        }
        return null;
    }


    /*
    * fusion://domain/2017-04-30/00/part-00000.gz
     * <p>
     * v2/img.yimutian.com_2017-04-30-00_part-00000.gz
    * */
    public FileStatus[] listStatus(Path path) throws IOException {
        log.info("list status for path: " + path.toString());
        String domain = path.toUri().getAuthority();
        String dirPath = path.toUri().getPath();

        String day;
        String hour;
        String[] filePathItems = dirPath.split("/");
        String fusionPath = null;
        boolean isGzFolder = false;
        switch (filePathItems.length) {
            case 3:
                isGzFolder = true;
                day = filePathItems[1];
                hour = filePathItems[2];
                fusionPath = String.format("%s/%s_%s-%s", FUSION_VERSION, domain, day, hour);
                break;
            case 2:
                day = filePathItems[1];
                fusionPath = String.format("%s/%s_%s", FUSION_VERSION, domain, day);
                break;
        }

        if (fusionPath != null && fusionPath.length() != 0) {
            log.info("find status for fusion dir path " + fusionPath);
            if (this.fusionLogItems != null && this.fusionLogItems.length > 0) {
                List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
                for (FusionLogger.LogItem item : this.fusionLogItems) {
                    if (item.name.startsWith(fusionPath)) {
                        String gzFileName = item.name.split("/")[1];//trim fusion version tag
                        String[] gzFileNameItems = gzFileName.split("_");
                        String itemDay = gzFileNameItems[1].substring(0, 10);
                        String itemHour = gzFileNameItems[1].substring(11, 13);
                        String itemPartName = gzFileNameItems[2];

                        if (isGzFolder) {
                            String fusionFsPath = String.format("/%s/%s/%s", itemDay, itemHour, itemPartName);
                            fileStatusList.add(new FileStatus(item.size, false, 0, 0, item.mtime * 1000,
                                    new Path(this.baseUri.getScheme(), this.baseUri.getAuthority(), fusionFsPath)));
                        } else {
                            String fusionFsPath = String.format("/%s/%s", itemDay, itemHour);
                            fileStatusList.add(new FileStatus(0, true, 0, 0, item.mtime * 1000,
                                    new Path(this.baseUri.getScheme(), this.baseUri.getAuthority(), fusionFsPath)));
                        }
                    }
                }

                return fileStatusList.toArray(new FileStatus[fileStatusList.size()]);
            }
        }
        return null;
    }

    public void setWorkingDirectory(Path path) {
        this.workingDir = path;
    }

    public Path getWorkingDirectory() {
        return this.workingDir;
    }

    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return false;
    }

    /*
         fusion://if-pbl.qiniudn.com/2017-05-22/00/part-00000.gz
     ->
         v2/if-pbl.qiniudn.com_2017-05-22-16_part-00000.gz
     */
    public FileStatus getFileStatus(Path path) throws IOException {
        log.info("get file status for " + path.toString());
        String domain = path.toUri().getAuthority();
        String filePath = path.toUri().getPath();

        String day;
        String hour;
        String gzFileName;
        String fusionPath = null;

        String[] filePathItems = filePath.split("/");
        if (filePath.endsWith(".gz") && filePathItems.length == 4) {
            //gz files
            day = filePathItems[1];
            hour = filePathItems[2];
            gzFileName = filePathItems[3];
            fusionPath = String.format("%s/%s_%s-%s_%s", FUSION_VERSION, domain, day, hour, gzFileName);
            log.info("find status for fusion file path " + fusionPath);

            if (this.fusionLogItems != null) {
                for (FusionLogger.LogItem item : this.fusionLogItems) {
                    if (item.name.equals(fusionPath)) {
                        return new FileStatus(item.size, false, 0, 0, item.mtime * 1000, path);
                    }
                }
            }
        } else {
            switch (filePathItems.length) {
                case 3:
                    day = filePathItems[1];
                    hour = filePathItems[2];
                    fusionPath = String.format("%s/%s_%s-%s", FUSION_VERSION, domain, day, hour);
                    break;
                case 2:
                    day = filePathItems[1];
                    fusionPath = String.format("%s/%s_%s", FUSION_VERSION, domain, day);
                    break;
            }
            if (fusionPath != null && fusionPath.length() != 0) {
                fusionPath = fusionPath + "/";
                log.info("find status for fusion dir path " + fusionPath);
                if (this.fusionLogItems != null && this.fusionLogItems.length > 0) {
                    FusionLogger.LogItem item = this.fusionLogItems[0];
                    return new FileStatus(0, true, 0, 0, item.mtime * 1000, path);
                }
            }
        }
        return null;
    }

    @Override
    public String getScheme() {
        return "fusion";
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        return super.globStatus(pathPattern);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
        return super.globStatus(pathPattern, filter);
    }

    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i,
                                     short i1, long l, Progressable progressable) throws IOException {
        return null;
    }

    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        return null;
    }

    public boolean rename(Path path, Path path1) throws IOException {
        return false;
    }

    public boolean delete(Path path, boolean b) throws IOException {
        return false;
    }

}
