package org.apache.bookkeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.client.BKException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BKPConstants {
    private final static Logger LOG = LoggerFactory.getLogger(BKPConstants.class);
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
    public static final byte SF_Error = 1; // Default error
    public static final byte SF_ErrorFenced = 10;
    public static final byte SF_ErrorChecksum = 11;
    public static final byte SF_ErrorReadOnly = 12;
    public static final byte SF_ErrorIncorrectParameter = 13;
    public static final byte SF_ErrorNoSuchExtent = 14;
    public static final byte SF_ErrorWrite = 15;
    public static final byte SF_ErrorRead = 16;
    public static final byte SF_ShortRead = 17;
    public static final byte SF_ErrorExist = 18;
    public static final byte SF_ServerTimeout = 19;
    public static final byte SF_ErrorExtentClosed = 20;
    public static final byte SF_ErrorAuth = 21;
    public static final byte SF_ErrorServerInterrupt = 22;
    public static final byte SF_ErrorMetaDataServer = 23;
    public static final byte SF_ErrorServerQuorum = 24;
    public static final byte SF_ErrorNoSuchFragmentClosed = 25;
    public static final byte SF_StatusInProgress = 26;
    public static final byte SF_ErrorUnknownVersion = 27;
    public static final byte SF_ErrorIllegalOperation = 28;
    public static final byte SF_ErrorNoSuchFragment = 29;


    // Defines
    public static final int EXTENTID_SIZE = 16;
    public static final int GENERIC_REQ_SIZE = 24;
    public static final int RESP_SIZE = 18;
    public static final int READ_REQ_SIZE = 8;
    public static final int WRITE_REQ_SIZE = 8;
    public static final int ASYNC_STAT_REQ_SIZE = 8;
    public static final long NO_ENTRY = -1;
    public static final byte UnInitialized = -1;
    // Version bumped to 3 due to change in ErrorCode response
    public static final short SFS_CURRENT_VERSION = 3;
    public static final short LEDGER_LIST_BATCH_SIZE = 100;

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
            return BKPConstants.SF_ErrorNoSuchFragment;
        case Code.NoSuchLedgerExistsException:
            return BKPConstants.SF_ErrorNoSuchExtent;
        case Code.LedgerClosedNoSuchEntryException:
            return BKPConstants.SF_ErrorNoSuchFragmentClosed;
        case Code.IncorrectParameterException:
            return BKPConstants.SF_ErrorIncorrectParameter;
        case Code.IllegalOpException:
            return BKPConstants.SF_ErrorIllegalOperation;
        case Code.InterruptedException:
            return BKPConstants.SF_ErrorServerInterrupt;
        case Code.LedgerFencedException:
            return BKPConstants.SF_ErrorFenced;
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
            LOG.error("Unmapped BK error: {}", BKerror);
            throw new RuntimeException("Unmapped BK error");
        }
    }

    public static byte getRespId(byte reqId) {
        switch (reqId) {
        case LedgerStatReq:
            return LedgerStatResp;
        case LedgerDeleteReq:
            return LedgerDeleteResp;
        case LedgerCreateReq:
            return LedgerCreateResp;
        case LedgerWriteCloseReq:
            return LedgerWriteCloseResp;
        case LedgerOpenRecoverReq:
            return LedgerOpenRecoverResp;
        case LedgerOpenReadReq:
            return LedgerOpenReadResp;
        case LedgerWriteEntryReq:
            return LedgerWriteEntryResp;
        case LedgerReadEntryReq:
            return LedgerReadEntryResp;
        case ReservedForFutureReq:
            return ReservedForFutureResp;
        case LedgerReadCloseReq:
            return LedgerReadCloseResp;
        case LedgerListGetReq:
            return LedgerListGetResp;
        case LedgerDeleteAllReq:
            return LedgerDeleteAllResp;
        case LedgerAsyncWriteEntryReq:
            return LedgerAsyncWriteEntryResp;
        case LedgerAsyncWriteStatusReq:
            return LedgerAsyncWriteStatusResp;
        case InvalidReq:
        default:
            return InvalidResp;
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
