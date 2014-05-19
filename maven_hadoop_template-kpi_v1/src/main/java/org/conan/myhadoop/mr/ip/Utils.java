package org.conan.myhadoop.mr.ip;

import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

/**
 * Created by zhou on 14-5-13.
 */
public class Utils {
    /**
     * 根据某种编码方式将字节数组转换成字符串
     * @param b 字节数组
     * @param offset 要转换的起始位置
     * @param len 要转换的长度
     * @param encoding 编码方式
     * @return 如果encoding不支持，返回一个缺省编码的字符串
     */
    public static String getString(byte[] b, int offset, int len, String encoding) {
        try {
            return new String(b, offset, len, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b, offset, len);
        }
    }

    /**
     *
     * 功能描述 IP数组转换成字符串
     *
     * @author  Administrator
     * <p>创建日期 ：2012-3-14 上午10:04:51</p>
     *
     * @param ip
     * 			ip的字节数组形式
     * @return
     * 			字符串形式的ip
     *
     * <p>修改历史 ：(修改人，修改时间，修改原因/内容)</p>
     */
    public static String getIpStringFromBytes(byte[] ip) {
        StringBuilder sb = new StringBuilder(256);
        sb.delete(0, sb.length());
        sb.append(ip[0] & 0xFF);
        sb.append('.');
        sb.append(ip[1] & 0xFF);
        sb.append('.');
        sb.append(ip[2] & 0xFF);
        sb.append('.');
        sb.append(ip[3] & 0xFF);
        return sb.toString();
    }

    /**
     * 从ip的字符串形式得到字节数组形式
     * @param ip 字符串形式的ip
     * @return 字节数组形式的ip
     */
    public static byte[] getIpByteArrayFromString(String ip) {
        byte[] ret = new byte[4];
        StringTokenizer st = new StringTokenizer(ip, ".");
        try {
            ret[0] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
            ret[1] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
            ret[2] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
            ret[3] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
        } catch (Exception e) {
            System.out.println("从ip的字符串形式得到字节数组形式报错" + e.toString());
        }
        return ret;
    }
}
