package com.kochudb.server;

import com.kochudb.types.KochuDoc;

public interface KVStorage {

    public KochuDoc get(byte[] key);

    public KochuDoc set(KochuDoc doc);

    public KochuDoc del(byte[] key);
}
