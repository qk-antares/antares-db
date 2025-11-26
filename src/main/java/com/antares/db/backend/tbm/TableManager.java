package com.antares.db.backend.tbm;

import com.antares.db.backend.dm.DataManager;
import com.antares.db.backend.parser.statement.Begin;
import com.antares.db.backend.parser.statement.Create;
import com.antares.db.backend.parser.statement.Delete;
import com.antares.db.backend.parser.statement.Insert;
import com.antares.db.backend.parser.statement.Select;
import com.antares.db.backend.parser.statement.Update;
import com.antares.db.backend.utils.Parser;
import com.antares.db.backend.vm.VersionManager;

public interface TableManager {
    /**
     * 开始一个事务，返回事务的xid
     */
    BeginRes begin(Begin begin);

    byte[] commit(long xid) throws Exception;

    byte[] abort(long xid);

    /**
     * 显示所有表结构
     */
    byte[] show(long xid);

    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;

    byte[] read(long xid, Select select) throws Exception;

    byte[] update(long xid, Update update) throws Exception;

    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManagerImpl create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManagerImpl open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
