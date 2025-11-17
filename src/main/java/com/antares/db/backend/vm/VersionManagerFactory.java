package com.antares.db.backend.vm;

import com.antares.db.backend.dm.DataManager;
import com.antares.db.backend.tm.TransactionManager;

public class VersionManagerFactory {
    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
