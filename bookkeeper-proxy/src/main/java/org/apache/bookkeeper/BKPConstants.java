package org.apache.bookkeeper;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.client.BKException.Code;

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
    public static final byte LedgerAsyncWriteEntryReq = 13;
    public static final byte LedgerAsyncWriteStatusReq = 14;
    public static final byte LedgerFirstUnusedReq = 15; // first unused request; please keep it up to date.
    public static final byte InvalidReq = 25;

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
    public static final byte LedgerAsyncWriteEntryResp = 113;
    public static final byte LedgerAsyncWriteStatusResp = 114;
    public static final byte InvalidResp = 125;

    // Error Codes
    public static final byte SF_OK = 0;
    public static final byte SF_Error = 10;
    public static final byte SF_ServerInternalError = 11;
    public static final byte SF_OutOfMemory = 12;
    public static final byte SF_ErrorChecksum = 13;
    public static final byte SF_ErrorReadOnly = 14;
    public static final byte SF_ErrorBadRequest = 15;
    public static final byte SF_ErrorNotFound = 16;
    public static final byte SF_ErrorWrite = 17;
    public static final byte SF_ErrorRead = 18;
    public static final byte SF_ShortREAD = 19;
    public static final byte SF_ErrorExist = 20;
    public static final byte SF_ServerTimeout = 21;
    public static final byte SF_ErrorExtentClosed = 22;
    public static final byte SF_ErrorAuth = 23;
    public static final byte SF_ErrorServerInterrupt = 24;
    public static final byte SF_ErrorMetaDataServer = 25;
    public static final byte SF_ErrorServerQuorum = 26;
    public static final byte SF_ErrorNotFoundClosed = 27;
    public static final byte SF_ErrorInProgress = 28;

    // Defines
    public static final int EXTENTID_SIZE = 16;
    public static final int GENERIC_REQ_SIZE = 24;
    public static final int RESP_SIZE = 10;
    public static final int READ_REQ_SIZE = 8;
    public static final int WRITE_REQ_SIZE = 8;
    public static final int ASYNC_STAT_REQ_SIZE = 8;
    public static final long NO_ENTRY = -1;
    public static final byte UnInitialized = -1;

    // Configuration values
    public static final long INFINITY = -1;

    public static final byte convertBKtoSFerror(int BKerror) {
        switch (BKerror) {
        case Code.OK:
            return BKPConstants.SF_OK;
        case Code.ReadException:
            return BKPConstants.SF_ErrorRead;
        case Code.QuorumException:
        case Code.NotEnoughBookiesException:
            return BKPConstants.SF_ErrorServerQuorum;
        case Code.ZKException:
        case Code.MetaStoreException:
            return BKPConstants.SF_ErrorMetaDataServer;
        case Code.LedgerClosedException:
            return BKPConstants.SF_ErrorExtentClosed;
        case Code.WriteException:
            return BKPConstants.SF_ErrorWrite;
        case Code.NoSuchEntryException:
        case Code.NoSuchLedgerExistsException:
            return BKPConstants.SF_ErrorNotFound;
        case Code.LedgerClosedNoSuchEntryException:
            return BKPConstants.SF_ErrorNotFoundClosed;
        case Code.IncorrectParameterException:
        case Code.IllegalOpException:
            return BKPConstants.SF_ErrorBadRequest;
        case Code.InterruptedException:
            return BKPConstants.SF_ErrorServerInterrupt;
        case Code.LedgerFencedException:
        case Code.WriteOnReadOnlyBookieException:
            return BKPConstants.SF_ErrorReadOnly;
        case Code.UnauthorizedAccessException:
            return BKPConstants.SF_ErrorAuth;
        case Code.AddEntryQuorumTimeoutException:
            return BKPConstants.SF_ServerTimeout;
        case Code.LedgerExistException:
        case Code.DuplicateEntryIdException:
            return BKPConstants.SF_ErrorExist;
        default:
            return BKPConstants.SF_ServerInternalError;
        }
    }

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
        ReqRespStringMap.put(LedgerAsyncWriteEntryReq, "LedgerAsyncWriteEntryReq");
        ReqRespStringMap.put(LedgerAsyncWriteEntryResp, "LedgerAsyncWriteEntryResp");
        ReqRespStringMap.put(LedgerAsyncWriteStatusReq, "LedgerAsyncWriteStatusReq");
        ReqRespStringMap.put(LedgerAsyncWriteStatusResp, "LedgerAsyncWriteStatusResp");
        ReqRespStringMap.put(InvalidReq, "InvalidReq");
        ReqRespStringMap.put(InvalidResp, "InvalidResp");
    };

    public static String getReqRespString(byte req) {
        String lstr = ReqRespStringMap.get(req);
        if (lstr == null) {
            return "UnKnownRequest/UnknowResponse";
        }
        return lstr;
    }
}
