package org.apache.bookkeeper.proxyclient;

import java.util.List;

public class ReturnValue {
    private final byte returnCode;

    ReturnValue(byte returnCode) {
        this.returnCode = returnCode;
    }

    public byte getReturnCode() {
        return returnCode;
    }

    public static class CreateExtentReturnValue extends ReturnValue {
        private byte[] extentId;

        CreateExtentReturnValue(byte returnCode, byte[] extentId) {
            super(returnCode);
            this.extentId = extentId;
        }

        public byte[] getExtentId() {
            return extentId;
        }

        public void setExtentId(byte[] extentId) {
            this.extentId = extentId;
        }
    }

    public static class AsyncWriteStatusReturnValue extends ReturnValue {
        private byte asyncWriteStatus;
        private long completionTime;

        AsyncWriteStatusReturnValue(byte returnCode) {
            super(returnCode);
        }

        public byte getAsyncWriteStatus() {
            return asyncWriteStatus;
        }

        public long getCompletionTime() {
            return completionTime;
        }

        public void setAsyncWriteStatus(byte asyncWriteStatus) {
            this.asyncWriteStatus = asyncWriteStatus;
        }

        public void setCompletionTime(long completionTime) {
            this.completionTime = completionTime;
        }
    }

    public static class ReadFragmentReturnValue extends ReturnValue {
        private int fragmentSize;
        private byte[] fragmentData;

        ReadFragmentReturnValue(byte returnCode) {
            super(returnCode);
        }

        public int getFragmentSize() {
            return fragmentSize;
        }

        public byte[] getFragmentData() {
            return fragmentData;
        }

        public void setFragmentSize(int fragmentSize) {
            this.fragmentSize = fragmentSize;
        }

        public void setFragmentData(byte[] fragmentData) {
            this.fragmentData = fragmentData;
        }
    }

    public static class ExtentStatReturnValue extends ReturnValue {
        private long extentLength;
        private long extentCTime;

        ExtentStatReturnValue(byte returnCode) {
            super(returnCode);
        }

        public long getExtentLength() {
            return extentLength;
        }

        public long getExtentCTime() {
            return extentCTime;
        }

        public void setExtentLength(long extentLength) {
            this.extentLength = extentLength;
        }

        public void setExtentCTime(long extentCTime) {
            this.extentCTime = extentCTime;
        }
    }

    public static class ExtentListGetReturnValue extends ReturnValue {
        private List<byte[]> extentIdsList;

        ExtentListGetReturnValue(byte returnCode) {
            super(returnCode);
        }

        public List<byte[]> getExtentIdsList() {
            return extentIdsList;
        }

        public void setExtentIdsList(List<byte[]> extentIdsList) {
            this.extentIdsList = extentIdsList;
        }
    }
}
