package com.lionxcat;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface GetNameRpcIF extends VersionedProtocol {
    long versionID = 1L;
    String findName(long studentId);
}
