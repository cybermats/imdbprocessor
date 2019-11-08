package org.cybermats.helpers;

public class InfoHelper {
    static public String parseString(String str) {
        return str;
    }

    static public Integer parseInt(String str) {
        if (str == null) {
            return null;
        }
        return Integer.parseInt(str);
    }

    static public Float parseFloat(String str) {
        if (str == null) {
            return null;
        }
        return Float.parseFloat(str);
    }

    static public String[] parseStringArray(String str) {
        if (str == null) {
            return null;
        }
        return str.split(",");
    }
}
