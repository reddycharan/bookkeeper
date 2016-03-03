package org.apache.bookkeeper.util;

import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatter to format a ledgerId
 */
public abstract class LedgerIdFormatter {

    private final static Logger LOG = LoggerFactory.getLogger(LedgerIdFormatter.class);

    protected Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

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

    public final static LedgerIdFormatter LONG_LEDGERID_FORMATTER = new LongLedgerIdFormatter();

    public static LedgerIdFormatter newLedgerIdFormatter(Configuration conf, String clsProperty) {
        String cls = conf.getString(clsProperty, LongLedgerIdFormatter.class.getName());
        ClassLoader classLoader = LedgerIdFormatter.class.getClassLoader();
        LedgerIdFormatter formatter;
        try {
            Class aCls = classLoader.loadClass(cls);
            formatter = (LedgerIdFormatter) aCls.newInstance();
            formatter.setConf(conf);
        } catch (Exception e) {
            LOG.warn("No formatter class found : " + cls, e);
            LOG.warn("Using Default Long LedgerId Formatter.");
            formatter = LONG_LEDGERID_FORMATTER;
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
