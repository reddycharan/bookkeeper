package org.apache.bookkeeper.util;

import java.util.UUID;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatter to format a ledgerId
 */
public abstract class LedgerIdFormatter {

    private final static Logger LOG = LoggerFactory.getLogger(LedgerIdFormatter.class);

    /**
     * Formats the LedgerId according to the type of the Formatter and return it
     * in String format
     *
     * @param ledgerId
     * @return
     */
    public abstract String formatLedgerId(long ledgerId);

    /**
     * converts the ledgeridString, which is in format of the type of formatter,
     * to the long value
     *
     * @param ledgerIdString
     * @return
     */
    public abstract long readLedgerId(String ledgerIdString);

    // Used by BKExtentIdByteArray
    public final static LedgerIdFormatter LONG_LEDGERID_FORMATTER = new LongLedgerIdFormatter();

    public static LedgerIdFormatter newLedgerIdFormatter(AbstractConfiguration conf) {
        LedgerIdFormatter formatter;
        try {
            formatter = ReflectionUtils.newInstance(conf.getLedgerIdFormatterClass());
        } catch (Exception e) {
            LOG.warn("No formatter class found", e);
            LOG.warn("Using Default UUID Formatter.");
            formatter = new UUIDLedgerIdFormatter();
        }
        return formatter;
    }

    public static LedgerIdFormatter newLedgerIdFormatter(String opt, AbstractConfiguration conf) {
        LedgerIdFormatter formatter;
        if ("hex".equals(opt)) {
                formatter = new LedgerIdFormatter.HexLedgerIdFormatter();
        }
        else if ("uuid".equals(opt)) {
            formatter = new LedgerIdFormatter.UUIDLedgerIdFormatter();
        }
        else if ("long".equals(opt)) {
            formatter = new LedgerIdFormatter.LongLedgerIdFormatter();
        }
        else {
            LOG.warn("specified unexpected ledgeridformat {}, so default LedgerIdFormatter is used", opt);
            formatter = newLedgerIdFormatter(conf);
        }
        return formatter;
    }

    public static class LongLedgerIdFormatter extends LedgerIdFormatter {

        @Override
        public String formatLedgerId(long ledgerId) {
            return Long.toString(ledgerId);
        }

        @Override
        public long readLedgerId(String ledgerIdString) {
            return Long.valueOf(ledgerIdString.trim());
        }
    }

    public static class HexLedgerIdFormatter extends LedgerIdFormatter {

        @Override
        public String formatLedgerId(long ledgerId) {
            return Long.toHexString(ledgerId);
        }

        @Override
        public long readLedgerId(String ledgerIdString) {
            return Long.valueOf(ledgerIdString.trim(), 16);
        }
    }

    public static class UUIDLedgerIdFormatter extends LedgerIdFormatter {

        @Override
        public String formatLedgerId(long ledgerId) {
            return (new UUID(0, ledgerId)).toString();
        }

        @Override
        public long readLedgerId(String ledgerIdString) {
            return UUID.fromString(ledgerIdString.trim()).getLeastSignificantBits();
        }
    }
}
