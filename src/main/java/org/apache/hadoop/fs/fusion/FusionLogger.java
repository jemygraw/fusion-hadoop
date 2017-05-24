package org.apache.hadoop.fs.fusion;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.util.Auth;
import com.qiniu.util.Json;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by jemy on 04/05/2017.
 */
public class FusionLogger {
    public static class LogItem {
        public String name;
        public long size;
        public long mtime;
        public String url;
    }

    public static class LogListResult {
        /**
         * 自定义状态码
         */
        public int code;
        /**
         * 自定义错误码描述，字符串
         */
        public String error;
        /**
         * 日志信息
         */
        public Map<String, LogItem[]> data;
    }

    private Auth auth;
    private Client client;

    public FusionLogger(String accessKey, String secretKey) {
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client();
    }


    public LogListResult getLogList(String[] domains, String logDate) throws QiniuException, UnsupportedEncodingException {
        StringMap req = new StringMap();
        req.put("domains", StringUtils.join(domains, ";"));
        req.put("day", logDate);

        byte[] body = Json.encode(req).getBytes("utf-8");
        String url = "http://fusion.qiniuapi.com/v2/tune/log/list";

        StringMap headers = auth.authorization(url);
        Response response = client.post(url, body, headers, Client.JsonMime);
        return response.jsonToObject(LogListResult.class);
    }
}
