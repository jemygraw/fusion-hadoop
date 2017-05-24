package org.apache.hadoop.fs.fusion;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;

/**
 * Created by jemy on 24/05/2017.
 */
public class FusionInputStream extends FSInputStream {
    private OkHttpClient fsClient;
    private long position;
    private FusionLogger.LogItem logItem;

    public FusionInputStream(OkHttpClient fsClient, FusionLogger.LogItem logItem) {
        this.fsClient = fsClient;
        this.logItem = logItem;
        this.position = 0L;
    }

    public synchronized void seek(long l) throws IOException {
        this.position = l;
    }

    public synchronized long getPos() throws IOException {
        return this.position;
    }

    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    public synchronized int read() throws IOException {
        byte[] buffer = new byte[1];
        return this.read(buffer, 0, 1);
    }

    public synchronized int read(byte[] buffer, int off, int len) throws IOException {
        if (this.position >= this.logItem.size) {
            //check the EOF of log file
            return -1;
        }
        Request getReq = (new Request.Builder())
                .url(this.logItem.url)
                .addHeader("Range", String.format("bytes=%s-%s", this.position, this.position + len - 1))
                .build();
        Response getResp = this.fsClient.newCall(getReq).execute();
        int statusCode = getResp.code();
        if (statusCode / 100 != 2) {
            throw new IOException("failed to get log file content, error " + getResp.message());
        }
        byte[] bodyBytes = getResp.body().bytes();
        System.arraycopy(bodyBytes, 0, buffer, off, bodyBytes.length);
        this.position += bodyBytes.length;
        return bodyBytes.length;
    }

}