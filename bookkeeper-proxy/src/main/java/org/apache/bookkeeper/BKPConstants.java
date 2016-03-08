package org.apache.bookkeeper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class BKPConstants {
    // Requests
    public static final byte LedgerStatReq = 1;
    public static final byte LedgerDeleteReq = 2;
    public static final byte LedgerCreateReq = 3;
    public static final byte LedgerWriteCloseReq = 4;
    public static final byte LedgerOpenRecoverReq = 5;
    public static final byte LedgerOpenReadReq = 6;
    public static final byte LedgerWriteEntryReq = 7;
    public static final byte LedgerReadEntryReq = 8;
    public static final byte ReservedForFutureReq = 9;
    public static final byte LedgerReadCloseReq = 10;
    public static final byte LedgerListGetReq = 11;
    public static final byte LedgerDeleteAllReq = 12;
    public static final byte LedgerWriteSafeCloseReq = 13;
    public static final byte LedgerReadSafeCloseReq = 14;

    // Responses
    public static final byte LedgerStatResp = 101;
    public static final byte LedgerDeleteResp = 102;
    public static final byte LedgerCreateResp = 103;
    public static final byte LedgerWriteCloseResp = 104;
    public static final byte LedgerOpenRecoverResp = 105;
    public static final byte LedgerOpenReadResp = 106;
    public static final byte LedgerWriteEntryResp = 107;
    public static final byte LedgerReadEntryResp = 108;
    public static final byte ReservedForFutureResp = 109;
    public static final byte LedgerReadCloseResp = 110;
    public static final byte LedgerListGetResp = 111;
    public static final byte LedgerDeleteAllResp = 112;
    public static final byte LedgerReadSafeCloseResp = 113;
    public static final byte LedgerWriteSafeCloseResp = 114;

    // Error Codes
    public static final byte SF_OK = 0;
    public static final byte SF_InternalError = 11;
    public static final byte SF_OutOfMemory = 12;
    public static final byte SF_ConnectionFailed = 13;
    public static final byte SF_ErrorPermanentRedirect = 14;
    public static final byte SF_ErrorBadRequest = 15;
    public static final byte SF_ErrorNotFound = 16;
    public static final byte SF_FailedIOerror = 17;
    public static final byte SF_PartialIO = 18;
    public static final byte SF_ShortREAD = 19;
    public static final byte SF_ErrorExist = 20;
    public static final byte SF_OutOfSequenceTimeout = 21;
    public static final byte SF_ErrorNotFoundClosed = 22;

    // Defines
    public static final int EXTENTID_SIZE = 16;
    public static final int GENERIC_REQ_SIZE = 24;
    public static final int RESP_SIZE = 10;
    public static final int READ_REQ_SIZE = 8;
    public static final int WRITE_REQ_SIZE = 8;
    public static final long NO_ENTRY = -1;
    public static final byte UnInitialized = -1;

    // Double brace initialization is sometimes evil but fits perfectly
    // for the current requirement.
    @SuppressWarnings("serial")
    private static final Map<Byte, String> ReqRespStringMap =
            Collections.unmodifiableMap(new HashMap<Byte, String>() {{
                this.put(UnInitialized, "== UNINITIALIZED ==");
                this.put(LedgerStatReq, "LedgerStatReq");
                this.put(LedgerStatResp, "LedgerStatResp");
                this.put(LedgerDeleteReq, "LedgerDeleteReq");
                this.put(LedgerDeleteResp, "LedgerDeleteResp");
                this.put(LedgerCreateReq, "LedgerCreateReq");
                this.put(LedgerCreateResp, "LedgerCreateResp");
                this.put(LedgerWriteCloseReq, "LedgerWriteCloseReq");
                this.put(LedgerWriteCloseResp, "LedgerWriteCloseResp");
                this.put(LedgerOpenRecoverReq, "LedgerOpenRecoverReq");
                this.put(LedgerOpenRecoverResp, "LedgerOpenRecoverResp");
                this.put(LedgerOpenReadReq, "LedgerOpenReadReq");
                this.put(LedgerOpenReadResp, "LedgerOpenReadResp");
                this.put(LedgerWriteEntryReq, "LedgerWriteEntryReq");
                this.put(LedgerWriteEntryResp, "LedgerWriteEntryResp");
                this.put(LedgerReadEntryReq, "LedgerReadEntryReq");
                this.put(LedgerReadEntryResp, "LedgerReadEntryResp");
                this.put(ReservedForFutureReq, "ReservedForFutureReq");
                this.put(ReservedForFutureResp, "ReservedForFutureResp");
                this.put(LedgerReadCloseReq, "LedgerReadCloseReq");
                this.put(LedgerReadCloseResp, "LedgerReadCloseResp");
                this.put(LedgerListGetReq, "LedgerListGetReq");
                this.put(LedgerListGetResp, "LedgerListGetResp");
                this.put(LedgerDeleteAllReq, "LedgerDeleteAllReq");
                this.put(LedgerDeleteAllResp, "LedgerDeleteAllResp");
                this.put(LedgerWriteSafeCloseReq, "LedgerWriteSafeCloseReq");
                this.put(LedgerWriteSafeCloseResp, "LedgerWriteSafeCloseResp");
                this.put(LedgerReadSafeCloseReq, "LedgerReadSafeCloseReq");
                this.put(LedgerReadSafeCloseResp, "LedgerReadSafeCloseResp");
            }});

    public static String getReqRespString(byte req) {
        String lstr = ReqRespStringMap.get(req);
        if (lstr == null) {
            return "UnKnownRequest/UnknowResponse";
        }
        return lstr;
    }
}
