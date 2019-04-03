package org.apache.bookkeeper.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PrimitiveIterator;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

/*
 * Ordered collection of SequenceGroups will represent entries of the ledger
 * residing in a bookie.
 *
 * In the byte array representation of AvailabilityOfEntriesOfLedger, for
 * the sake of future extensibility it would be helpful to have reserved
 * space for header at the beginning. So the first 64 bytes will be used for
 * header, with the first four bytes specifying the int version number, next
 * four bytes specifying the number of sequencegroups for now and the rest
 * of the bytes in the reserved space will be 0's. The encoded format will
 * be represented after the first 64 bytes. The ordered collection of
 * SequenceGroups will be appended sequentially to this byte array, with
 * each SequenceGroup taking 24 bytes.
 */
public class AvailabilityOfEntriesOfLedger {
    public static final long INVALID_ENTRYID = -1;

    /*
     *
     * Nomenclature:
     *
     * - Continuous entries are grouped as a ’Sequence’. - Number of continuous
     * entries in a ‘Sequence’ is called ‘sequenceSize’. - Gap between
     * Consecutive sequences is called ‘sequencePeriod’. - Consecutive sequences
     * with same sequenceSize and same sequencePeriod in between consecutive
     * sequences are grouped as a SequenceGroup. - ‘firstSequenceStart’ is the
     * first entry in the first sequence of the SequenceGroup. -
     * ‘lastSequenceStart’ is the first entry in the last sequence of the
     * SequenceGroup.
     *
     * To represent a SequenceGroup, two long values and two int values are
     * needed, so each SequenceGroup can be represented with (2 * 8 + 2 * 4 = 24
     * bytes).
     */
    private static class SequenceGroup {
        private final long firstSequenceStart;
        private int sequenceSize;
        long lastSequenceStart = INVALID_ENTRYID;
        int sequencePeriod;
        static final int SEQUENCEGROUP_BYTES = 2 * Long.BYTES + 2 * Integer.BYTES;

        private SequenceGroup(long firstSequenceStart, int sequenceSize) {
            this.firstSequenceStart = firstSequenceStart;
            this.sequenceSize = sequenceSize;
        }

        void serializeSequenceGroup(byte[] byteArrayForSerialization) {
            ByteBuffer buffer = ByteBuffer.wrap(byteArrayForSerialization);
            buffer.putLong(firstSequenceStart);
            buffer.putLong(lastSequenceStart);
            buffer.putLong(sequenceSize);
            buffer.putLong(sequencePeriod);
        }
    }

    static final int HEADER_SIZE = 64;
    static final int V0 = 0;
    // current version of AvailabilityOfEntriesOfLedger header is V0
    public static final int CURRENT_HEADER_VERSION = V0;
    ArrayList<SequenceGroup> listOfSequenceGroups = new ArrayList<SequenceGroup>();
    MutableObject<SequenceGroup> curSequenceGroup = new MutableObject<SequenceGroup>(null);
    MutableLong curSequenceStartEntryId = new MutableLong(INVALID_ENTRYID);
    MutableInt curSequenceSize = new MutableInt(0);

    public AvailabilityOfEntriesOfLedger(PrimitiveIterator.OfLong entriesOfLedgerItr) {
        while (entriesOfLedgerItr.hasNext()) {
            this.addEntryToAvailabileEntriesOfLedger(entriesOfLedgerItr.nextLong());
        }
        this.closeStateOfEntriesOfALedger();
    }

    private void initializeCurSequence(long curSequenceStartEntryIdValue) {
        curSequenceStartEntryId.setValue(curSequenceStartEntryIdValue);
        curSequenceSize.setValue(1);
    }

    private void resetCurSequence() {
        curSequenceStartEntryId.setValue(INVALID_ENTRYID);
        curSequenceSize.setValue(0);
    }

    private boolean isCurSequenceInitialized() {
        return curSequenceStartEntryId.longValue() != INVALID_ENTRYID;
    }

    private boolean isEntryExistingInCurSequence(long entryId) {
        return (curSequenceStartEntryId.longValue() <= entryId)
                && (entryId < (curSequenceStartEntryId.longValue() + curSequenceSize.intValue()));
    }

    private boolean isEntryAppendableToCurSequence(long entryId) {
        return ((curSequenceStartEntryId.longValue() + curSequenceSize.intValue()) == entryId);
    }

    private void incrementCurSequenceSize() {
        curSequenceSize.increment();
    }

    private void createNewSequenceGroupWithCurSequence() {
        SequenceGroup curSequenceGroupValue = curSequenceGroup.getValue();
        if (curSequenceGroupValue.lastSequenceStart == INVALID_ENTRYID) {
            curSequenceGroupValue.lastSequenceStart = curSequenceGroupValue.firstSequenceStart;
            curSequenceGroupValue.sequencePeriod = 0;
        }
        listOfSequenceGroups.add(curSequenceGroupValue);
        curSequenceGroup.setValue(new SequenceGroup(curSequenceStartEntryId.longValue(), curSequenceSize.intValue()));
    }

    private boolean isCurSequenceGroupInitialized() {
        return curSequenceGroup.getValue() != null;
    }

    private void initializeCurSequenceGroupWithCurSequence() {
        curSequenceGroup.setValue(new SequenceGroup(curSequenceStartEntryId.longValue(), curSequenceSize.intValue()));
    }

    private boolean doesCurSequenceBelongToCurSequenceGroup() {
        long curSequenceStartEntryIdValue = curSequenceStartEntryId.longValue();
        int curSequenceSizeValue = curSequenceSize.intValue();
        boolean belongsToThisSequenceGroup = false;
        SequenceGroup curSequenceGroupValue = curSequenceGroup.getValue();
        if ((curSequenceGroupValue.sequenceSize == curSequenceSizeValue)
                && ((curSequenceGroupValue.lastSequenceStart == INVALID_ENTRYID) || ((curSequenceStartEntryIdValue
                        - curSequenceGroupValue.lastSequenceStart) == curSequenceGroupValue.sequencePeriod))) {
            belongsToThisSequenceGroup = true;
        }
        return belongsToThisSequenceGroup;
    }

    private void appendCurSequenceToCurSequenceGroup() {
        SequenceGroup curSequenceGroupValue = curSequenceGroup.getValue();
        if (curSequenceGroupValue.lastSequenceStart == INVALID_ENTRYID) {
            curSequenceGroupValue.lastSequenceStart = curSequenceStartEntryId.longValue();
            curSequenceGroupValue.sequencePeriod = ((int) (curSequenceGroupValue.lastSequenceStart
                    - curSequenceGroupValue.firstSequenceStart));
        } else {
            curSequenceGroupValue.lastSequenceStart = curSequenceStartEntryId.longValue();
        }
    }

    private void addCurSequenceToSequenceGroup() {
        if (!isCurSequenceGroupInitialized()) {
            initializeCurSequenceGroupWithCurSequence();
        } else if (doesCurSequenceBelongToCurSequenceGroup()) {
            appendCurSequenceToCurSequenceGroup();
        } else {
            createNewSequenceGroupWithCurSequence();
        }
    }

    private void addEntryToAvailabileEntriesOfLedger(long entryId) {
        if (!isCurSequenceInitialized()) {
            initializeCurSequence(entryId);
        } else if (isEntryExistingInCurSequence(entryId)) {
            /* this entry is already added so do nothing */
        } else if (isEntryAppendableToCurSequence(entryId)) {
            incrementCurSequenceSize();
        } else {
            addCurSequenceToSequenceGroup();
            initializeCurSequence(entryId);
        }
    }

    private void closeStateOfEntriesOfALedger() {
        if (isCurSequenceInitialized()) {
            addCurSequenceToSequenceGroup();
            resetCurSequence();
        }
    }

    public byte[] serializeStateOfEntriesOfALedger() {
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer headerByteBuf = ByteBuffer.wrap(header);
        byte[] serializedSequenceGroupByteArray = new byte[SequenceGroup.SEQUENCEGROUP_BYTES];
        byte[] serializedStateByteArray = new byte[HEADER_SIZE
                + (listOfSequenceGroups.size() * SequenceGroup.SEQUENCEGROUP_BYTES)];
        final int numOfSequenceGroups = listOfSequenceGroups.size();
        headerByteBuf.putInt(CURRENT_HEADER_VERSION);
        headerByteBuf.putInt(numOfSequenceGroups);
        System.arraycopy(header, 0, serializedStateByteArray, 0, HEADER_SIZE);
        int seqNum = 0;
        for (SequenceGroup seqGroup : listOfSequenceGroups) {
            Arrays.fill(serializedSequenceGroupByteArray, (byte) 0);
            seqGroup.serializeSequenceGroup(serializedSequenceGroupByteArray);
            System.arraycopy(serializedSequenceGroupByteArray, 0, serializedStateByteArray,
                    HEADER_SIZE + ((seqNum++) * SequenceGroup.SEQUENCEGROUP_BYTES), SequenceGroup.SEQUENCEGROUP_BYTES);
        }
        return serializedStateByteArray;
    }
}
