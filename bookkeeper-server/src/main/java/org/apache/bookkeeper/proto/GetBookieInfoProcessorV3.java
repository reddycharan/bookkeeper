package org.apache.bookkeeper.proto;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetBookieInfoProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(GetBookieInfoProcessorV3.class);

    public GetBookieInfoProcessorV3(Request request, Channel channel,
                                     BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    private GetBookieInfoResponse getGetBookieInfoResponse() {
        long startTimeNanos = MathUtils.nowInNano();
        GetBookieInfoRequest getBookieInfoRequest = request.getGetBookieInfoRequest();
        long requested = getBookieInfoRequest.getRequested();

        GetBookieInfoResponse.Builder getBookieInfoResponse = GetBookieInfoResponse.newBuilder();

        if (!isVersionCompatible()) {
            getBookieInfoResponse.setStatus(StatusCode.EBADVERSION);
            requestProcessor.getBookieInfoStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            return getBookieInfoResponse.build();
        }

        LOG.debug("Received new getBookieInfo request: {}", request);
        StatusCode status = StatusCode.EOK;
        long freeDiskSpace = 0L, totalDiskSpace = 0L;
        if ((requested & GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
            freeDiskSpace = requestProcessor.bookie.getTotalFreeSpace();
            getBookieInfoResponse.setFreeDiskSpace(freeDiskSpace);
        }
        if ((requested & GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE) != 0) {
            totalDiskSpace = requestProcessor.bookie.getTotalDiskSpace();
            getBookieInfoResponse.setTotalDiskCapacity(totalDiskSpace);
        }
        LOG.debug("FreeDiskSpace info is " + freeDiskSpace + " totalDiskSpace is: " + totalDiskSpace);
        getBookieInfoResponse.setStatus(status);
        requestProcessor.getBookieInfoStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                TimeUnit.NANOSECONDS);
        return getBookieInfoResponse.build();
    }

    @Override
    public void run() {
        GetBookieInfoResponse getBookieInfoResponse = getGetBookieInfoResponse();
        sendResponse(getBookieInfoResponse);
    }

    private void sendResponse(GetBookieInfoResponse getBookieInfoResponse) {
        Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(getBookieInfoResponse.getStatus())
                .setGetBookieInfoResponse(getBookieInfoResponse);
        sendResponse(response.getStatus(),
                     response.build(),
                     requestProcessor.getBookieInfoStats);
    }
}
