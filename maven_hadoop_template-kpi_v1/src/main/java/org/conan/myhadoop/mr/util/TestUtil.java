package org.conan.myhadoop.mr.util;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhou on 14-5-16.
 */
public class TestUtil {
    public static void main(String[] args) throws Exception {
//        String request = "/archiver/tid-753810.html?page=3";
//        System.out.println(" url index of ? " + request.indexOf("?"));
//        if(request.indexOf("?")>0){
//            System.out.println(" url " + request.split("\\?")[0]);
//        }
        Set<String> count = new HashSet<String>();
        count.add("192.168.1.40");
        System.out.println(" 1 " + count.size());
        count.add("192.168.1.40");
        System.out.println(" 2 " + count.size());
    }
}
