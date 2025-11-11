package com.antares.db.backend.dm.dateItem;

public interface DataItem {
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
