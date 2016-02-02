package org.apache.bookkeeper.util;

import java.io.IOException;

import org.apache.commons.io.HexDump;

public class HexDumpEntryFormatter extends EntryFormatter {
    @Override
    public void formatEntry(byte[] data) {
        try {
            HexDump.dump(data, 0, System.out, 0);
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Warn: Index is outside the data array's bounds : " + e.getMessage());
        } catch (IllegalArgumentException e) {
            System.out.println("Warn: The output stream is null : " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Warn: Something has gone wrong writing the data to stream : " + e.getMessage());
        }
    }

    @Override
    public void formatEntry(java.io.InputStream input) {
        try {
            byte[] data = new byte[input.available()];
            input.read(data, 0, data.length);
            formatEntry(data);
        } catch (IOException ie) {
            System.out.println("Warn: Unreadable entry : " + ie.getMessage());
        }
    }

};
