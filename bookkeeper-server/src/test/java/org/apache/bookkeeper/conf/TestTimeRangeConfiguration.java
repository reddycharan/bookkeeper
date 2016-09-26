package org.apache.bookkeeper.conf;

import static org.junit.Assert.*;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.util.List;

import org.junit.Test;

public class TestTimeRangeConfiguration {

    @Test
    public void parseTimeRanges() {
        ServerConfiguration conf = new ServerConfiguration();

        conf.setProperty("timerange0.start", "08:00");
        conf.setProperty("timerange0.end", "08:59:59.999");

        conf.setProperty("timerange1.start", "09:00");
        conf.setProperty("timerange1.end", "13:59:59.999");
        conf.setProperty("timerange1.days", "Saturday,Sunday");

        // should be ignored because timerange2 does not exist
        conf.setProperty("timerange3.start", "00:00");
        conf.setProperty("timerange3.end", "23:59:59.999");

        LocalTime now = LocalTime.of(8, 59);
        LocalTime later = LocalTime.of(9, 00);

        for (int day = 1; day <= 5; day++) { // weekdays
            List<String> ranges = conf.getActiveDailyRanges(now, DayOfWeek.of(day));
            assertEquals("only range 0 is applicable", 1, ranges.size());
            assertEquals("only timerange0 exists for weekdays / current time", "timerange0", ranges.get(0));

            ranges = conf.getActiveDailyRanges(later, DayOfWeek.of(day));
            assertEquals("no applicable ranges", 0, ranges.size());
        }

        now = LocalTime.of(9, 00);
        later = LocalTime.of(14, 00);
        for (int day = 6; day <= 7; day++) { // weekend
            List<String> ranges = conf.getActiveDailyRanges(now, DayOfWeek.of(day));
            assertEquals("only range 1 is applicable", 1, ranges.size());
            assertEquals("timerange1 exists for weekend / current time", "timerange1", ranges.get(0));

            ranges = conf.getActiveDailyRanges(later, DayOfWeek.of(day));
            assertEquals("no applicable ranges", 0, ranges.size());
        }
    }

    @Test
    public void malformedTimeRangeTime() {
        LocalTime now = LocalTime.of(13, 59);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty("timerange0.start", "09:00");

        conf.setProperty("timerange1.start", "09:00");
        conf.setProperty("timerange1.end", "13:59:59.999");

        List<String> ranges = conf.getActiveDailyRanges(now, DayOfWeek.MONDAY);
        assertEquals("no valid ranges, parsing stops on first error", 0, ranges.size());
    }

    @Test
    public void malformedTimeRangeTime2() {
        LocalTime now = LocalTime.of(13, 59);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty("timerange0.start", "09:00");
        conf.setProperty("timerange1.end", "15:00PM");

        conf.setProperty("timerange1.start", "09:00");
        conf.setProperty("timerange1.end", "13:59:59.999");

        List<String> ranges = conf.getActiveDailyRanges(now, DayOfWeek.MONDAY);
        assertEquals("no valid ranges, parsing stops on first error", 0, ranges.size());
    }

    @Test
    public void malformedTimeRangeStartAfterEnd() {
        LocalTime now = LocalTime.of(13, 59);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty("timerange0.start", "13:59:59.999");
        conf.setProperty("timerange0.end", "09:00");

        conf.setProperty("timerange1.start", "09:00");
        conf.setProperty("timerange1.end", "13:59:59.999");

        List<String> ranges = conf.getActiveDailyRanges(now, DayOfWeek.MONDAY);
        assertEquals("no valid ranges, parsing stops on first error", 0, ranges.size());
    }

    @Test
    public void overlappingRanges() {
        LocalTime now = LocalTime.of(8, 59);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty("timerange0.start", "07:59:59.999");
        conf.setProperty("timerange0.end", "09:00");

        conf.setProperty("timerange1.start", "08:59");
        conf.setProperty("timerange1.end", "13:59:59.999");

        List<String> ranges = conf.getActiveDailyRanges(now, DayOfWeek.MONDAY);
        assertEquals("got both ranges", 2, ranges.size());
    }

    @Test
    public void malformedTimeRangeDay() {
        LocalTime now = LocalTime.of(13, 59);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty("timerange0.start", "09:00");
        conf.setProperty("timerange0.end", "13:59:59.999");
        conf.setProperty("timerange0.days", "Caturday,Sunday");

        conf.setProperty("timerange1.start", "09:00");
        conf.setProperty("timerange1.end", "13:59:59.999");

        List<String> ranges = conf.getActiveDailyRanges(now, DayOfWeek.MONDAY);
        assertEquals("no valid ranges, parsing stops on first error", 0, ranges.size());
    }

    @Test
    public void findPropertyForCurrentTimerange() {
        ServerConfiguration conf = new ServerConfiguration();

        conf.setProperty("timerange0.start", "09:00");
        conf.setProperty("timerange0.end", "14:00");

        conf.setProperty("timerange1.start", "20:00");
        conf.setProperty("timerange1.end", "23:59:59.999");
        conf.setProperty("timerange1.days", "Saturday,Sunday");

        conf.setProperty("testprop", "-1");
        conf.setProperty("testprop@timerange0", "0");
        conf.setProperty("testprop@timerange1", "1");

        LocalTime inRange0 = LocalTime.of(13, 59);
        LocalTime inRange1 = LocalTime.of(20, 1);
        LocalTime outOfRanges = LocalTime.of(1, 1);

        assertEquals("in range 0", "0",
                conf.getString(conf.getPropertyNameForFirstActiveTimerange("testprop", inRange0, DayOfWeek.SATURDAY)));

        assertEquals("in range 1", "1",
                conf.getString(conf.getPropertyNameForFirstActiveTimerange("testprop", inRange1, DayOfWeek.SATURDAY)));

        assertEquals("out of ranges", "-1", conf
                .getString(conf.getPropertyNameForFirstActiveTimerange("testprop", outOfRanges, DayOfWeek.SATURDAY)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void configurationValidation() {
        final ServerConfiguration conf = new ServerConfiguration();
        final ConfigurationValidator cv = new ConfigurationValidator() {
            @Override
            public void validateAtTimeAndDay(LocalTime now, DayOfWeek day) throws IllegalArgumentException {
                long a = conf.getLong(conf.getPropertyNameForFirstActiveTimerange("propA", now, day));
                long b = conf.getLong(conf.getPropertyNameForFirstActiveTimerange("propB", now, day));

                if (a + b > 0) {
                    throw new IllegalArgumentException("bad config");
                }
            }
        };
        
        conf.setProperty("timerange0.start", "09:00");
        conf.setProperty("timerange0.end", "14:00");

        conf.setProperty("propA", "1");
        conf.setProperty("propB", "-1");

        try {
            conf.validateParametersForAllTimeRanges(cv);
        } catch(IllegalArgumentException e) {
            fail("validation failure");
        }

        conf.setProperty("propB@timerange0", "1");
        conf.validateParametersForAllTimeRanges(cv);
    }
}
