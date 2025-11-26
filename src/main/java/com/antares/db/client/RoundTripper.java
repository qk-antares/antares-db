package com.antares.db.client;

import com.antares.db.transport.Package;
import com.antares.db.transport.Packager;

/**
 * 对Packager再次封装，实现单次收发动作
 */
public class RoundTripper {
    private Packager packager;
    
    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
