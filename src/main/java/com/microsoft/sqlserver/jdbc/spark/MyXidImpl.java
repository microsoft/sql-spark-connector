package com.microsoft.sqlserver.jdbc.spark;

import javax.transaction.xa.Xid;

public class MyXidImpl implements Xid {

    public int formatId;
    public byte[] gtrid;
    public byte[] bqual;

    public int getFormatId() {
        return formatId;
    }

    public byte[] getGlobalTransactionId() {
        return gtrid;
    }

    public byte[] getBranchQualifier() {
        return bqual;
    }

    public MyXidImpl(int formatId, byte[] gtrid, byte[] bqual) {
        this.formatId = formatId;
        this.gtrid = gtrid;
        this.bqual = bqual;
    }
}