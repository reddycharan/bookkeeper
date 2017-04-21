package com.salesforce.storagecloud;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import javax.naming.directory.InvalidAttributeValueException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import pl.otros.logview.api.InitializationException;
import pl.otros.logview.api.batch.BatchProcessingContext;
import pl.otros.logview.api.batch.LogDataParsedListener;
import pl.otros.logview.api.importer.LogImporterUsingParser;
import pl.otros.logview.api.model.LogData;
import pl.otros.logview.api.parser.ParsingContext;
import pl.otros.logview.batch.StreamProcessingLogDataCollector;
import pl.otros.logview.parser.log4j.Log4jPatternMultilineLogParser;


/**
 * Format SFStore proxy logs into avro records
 *
 */
public class ProxyLogFormatter implements LogDataParsedListener
{
    private String inputFilepath;
    private String outputFilepath;
    private DatumWriter<BKProxyLogRecord> writer;
    private DataFileWriter<BKProxyLogRecord> fwriter;
    private InputStream ins;

    private File out;
    private File in;

    private long totalCount = 0;

    private final String PATTERN = "TIMESTAMP [LEVEL] [THREAD] [FILE:LINE] - MESSAGE";
    private final String DATEFORMAT = "yyyyMMddHHmmss.SSSSSS";
    private final String TYPE = "log4j";

    public ProxyLogFormatter(String inFile, String outFile) {
        this.inputFilepath = inFile;
        in = new File(inputFilepath);

        this.outputFilepath = outFile;
        out = new File(outputFilepath);
    }

    public void init() throws InvalidAttributeValueException, IOException, FileNotFoundException {
        writer = new SpecificDatumWriter<BKProxyLogRecord>(BKProxyLogRecord.class);
        fwriter = new DataFileWriter<BKProxyLogRecord>(writer);

        fwriter.create((new BKProxyLogRecord()).getSchema(), out);
        ins = new FileInputStream(in);
    }

    public void close() throws IOException {
        fwriter.close();
        ins.close();
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void parse() throws InitializationException, FileNotFoundException, IOException {
        Properties p = new Properties();
        p.put("type", TYPE);
        p.put("pattern", PATTERN);
        p.put("dateFormat", DATEFORMAT);
        Log4jPatternMultilineLogParser logParser = new Log4jPatternMultilineLogParser();
        LogImporterUsingParser importerUsingParser = new LogImporterUsingParser(logParser);

        importerUsingParser.init(p);
        ParsingContext context = new ParsingContext();
        logParser.initParsingContext(context);

        BatchProcessingContext batchProcessingContext = new BatchProcessingContext();
        StreamProcessingLogDataCollector streamDataCollector = new StreamProcessingLogDataCollector(this,
                batchProcessingContext);

        // parse log file
        importerUsingParser.importLogs(ins, streamDataCollector, context);
    }

    public static void main(String[] args)
    {
        /* parse args */
        CommandLineParser argParser = new DefaultParser();
        Options opts = new Options();
        HelpFormatter helpFormatter = new HelpFormatter();

        opts.addOption(Option.builder("o")
                .longOpt("output")
                .desc("output file path")
                .hasArg()
                .argName("PATH")
                .numberOfArgs(1)
                .build());

        opts.addOption(Option.builder("p")
                .longOpt("proxy-hostname")
                .desc("proxy hostname")
                .hasArg()
                .required()
                .argName("hostname")
                .numberOfArgs(1)
                .build());

        opts.addOption(Option.builder("h")
                .longOpt("help")
                .build());

        try {
            CommandLine cmdLine = argParser.parse(opts, args);
            String outputFile = cmdLine.getOptionValue("output", "proxy_avrorecords.dat");
            String inputFile;
            ProxyLogFormatter logFormatter;
            String[] params = cmdLine.getArgs();

            if (cmdLine.hasOption("help")) {
                helpFormatter.printHelp("proxylogformatter LOG", opts, true);
                return;
            }

            if (params.length != 1) {
                helpFormatter.printHelp("proxylogformatter LOG", opts, true);
                System.exit(-1);
            }
            inputFile = params[0];

            logFormatter = new ProxyLogFormatter(inputFile, outputFile);
            logFormatter.init();
            logFormatter.parse();

            logFormatter.close();
            System.out.println("total records: " + logFormatter.getTotalCount());
        } catch (ParseException e) {
            helpFormatter.printHelp("proxylogformatter LOG", opts, true);
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public void logDataParsed(LogData logData, BatchProcessingContext batch) throws IOException {
        totalCount++;

        /* populate fields */
        Date dateTime = logData.getDate();
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateTime);
        BKProxyLogRecord avroRecord = BKProxyLogRecord.newBuilder()
                .setLogStartYear(cal.get(Calendar.YEAR))
                .setLogStartMonth(cal.get(Calendar.MONTH) + 1).setLogStartDay(cal.get(Calendar.DATE))
                .setLogStartHour(cal.get(Calendar.HOUR)).setLogStartMin(cal.get(Calendar.MINUTE))
                .setLogStartSec(cal.get(Calendar.SECOND))
                .setLogFilename((CharSequence) in.getName())
                .setLogHostname((CharSequence) "")

                .setTime(logData.getDate().getTime())
                .setType((CharSequence) logData.getLevel().getName())
                .setThread((CharSequence) logData.getThread())
                .setSrcFile((CharSequence) logData.getFile())
                .setSrcLinenum(Integer.parseInt(logData.getLine()))
                .setCorrId((CharSequence) "")
                .setExtentid((CharSequence) "").setExtentidLong(0)
                .setFragmentid(0).setContent(logData.getMessage())
                .setFreeformString("")
                .build();

        fwriter.append(avroRecord);
    }
}
