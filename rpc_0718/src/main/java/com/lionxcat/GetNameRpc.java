package com.lionxcat;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.hadoop.ipc.ProtocolSignature;

public class GetNameRpc implements GetNameRpcIF {
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return GetNameRpcIF.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
            throws IOException {
        return new ProtocolSignature(GetNameRpcIF.versionID, null);
    }

    @Override
    public String findName(long studentId) {
        System.out.println("[server] id: " + studentId);
        return data.get(studentId);
    }

    private Dictionary<Long, String> data;

    public GetNameRpc() {
        data = new Hashtable<Long, String>();
        data.put(20210123456789L, "心心");
        data.put(20210607020479L, "暴筱");
        data.put(202120212021L, "Hello World!");
    }
}
