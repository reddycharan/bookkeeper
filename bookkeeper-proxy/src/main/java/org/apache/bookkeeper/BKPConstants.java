package org.apache.bookkeeper;

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

    private static Map<Byte, String> ReqRespStringMap = new HashMap<Byte, String>();
    static {
        ReqRespStringMap.put(UnInitialized, "== UNINITIALIZED ==");
        ReqRespStringMap.put(LedgerStatReq, "LedgerStatReq");
        ReqRespStringMap.put(LedgerStatResp, "LedgerStatResp");
        ReqRespStringMap.put(LedgerDeleteReq, "LedgerDeleteReq");
        ReqRespStringMap.put(LedgerDeleteResp, "LedgerDeleteResp");
        ReqRespStringMap.put(LedgerCreateReq, "LedgerCreateReq");
        ReqRespStringMap.put(LedgerCreateResp, "LedgerCreateResp");
        ReqRespStringMap.put(LedgerWriteCloseReq, "LedgerWriteCloseReq");
        ReqRespStringMap.put(LedgerWriteCloseResp, "LedgerWriteCloseResp");
        ReqRespStringMap.put(LedgerOpenRecoverReq, "LedgerOpenRecoverReq");
        ReqRespStringMap.put(LedgerOpenRecoverResp, "LedgerOpenRecoverResp");
        ReqRespStringMap.put(LedgerOpenReadReq, "LedgerOpenReadReq");
        ReqRespStringMap.put(LedgerOpenReadResp, "LedgerOpenReadResp");
        ReqRespStringMap.put(LedgerWriteEntryReq, "LedgerWriteEntryReq");
        ReqRespStringMap.put(LedgerWriteEntryResp, "LedgerWriteEntryResp");
        ReqRespStringMap.put(LedgerReadEntryReq, "LedgerReadEntryReq");
        ReqRespStringMap.put(LedgerReadEntryResp, "LedgerReadEntryResp");
        ReqRespStringMap.put(ReservedForFutureReq, "ReservedForFutureReq");
        ReqRespStringMap.put(ReservedForFutureResp, "ReservedForFutureResp");
        ReqRespStringMap.put(LedgerReadCloseReq, "LedgerReadCloseReq");
        ReqRespStringMap.put(LedgerReadCloseResp, "LedgerReadCloseResp");
        ReqRespStringMap.put(LedgerListGetReq, "LedgerListGetReq");
        ReqRespStringMap.put(LedgerListGetResp, "LedgerListGetResp");
        ReqRespStringMap.put(LedgerDeleteAllReq, "LedgerDeleteAllReq");
        ReqRespStringMap.put(LedgerDeleteAllResp, "LedgerDeleteAllResp");
    };

    public static String getReqRespString(byte req) {
        String lstr = ReqRespStringMap.get(req);
        if (lstr == null) {
            return "UnKnownRequest/UnknowResponse";
        }
        return lstr;
    }
}
