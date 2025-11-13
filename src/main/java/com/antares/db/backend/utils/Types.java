package com.antares.db.backend.utils;

public class Types {
    /**
     * [pgno(4)][空位(2)[offset(2)]]
     * @param pgno
     * @param offset
     * @return
     */
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long) pgno;
        long u1 = (long) offset;
        return u0 << 32 | u1;
    }
}
