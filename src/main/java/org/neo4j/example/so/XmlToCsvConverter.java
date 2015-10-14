package org.neo4j.example.so;

import au.com.bytecode.opencsv.CSVWriter;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

/**
 * @author mh
 * @since 11.10.15
 */
public class XmlToCsvConverter implements Runnable {

    private static final int MB = 1024 * 1024;

    private static final int SAMPLE_ROWS = 100;

    private static final int SKIP_TEXT_SIZE = 255;
    private static final String CLEANUP_REGEXP = "['\"\r\n\\\\]+";

    private static final ExecutorService pool = Executors.newFixedThreadPool(4);
    private static final XMLInputFactory FACTORY = XMLInputFactory.newInstance();

    private final String file;
    private final String[] names;
    private final ProcessCallback callback;

    public XmlToCsvConverter(String file, String[] names, ProcessCallback callback) {

        this.file = file;
        this.names = names;
        this.callback = callback==null ? ProcessCallback.NONE : callback;
    }

    public static void main(String[] args) throws IOException, XMLStreamException, InterruptedException {
        for (String arg : args) {
            String[] parts = arg.split(":");
            String[] names = (parts.length > 1) ? parts[1].split(",") : null;
            pool.execute(new XmlToCsvConverter(parts[0], names,null));
        }
        pool.shutdown();
        pool.awaitTermination(1000, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        String baseName = file.substring(0, file.indexOf('.'));
        System.out.println("Processing "+file);
        callback.start(baseName);
        boolean mappingProvided = isMappingProvided();
        Map<String, Integer> mapping = createMapping();
        String[] line = new String[mapping.size()];

        int row = 0;
        long start = System.currentTimeMillis();
        try {
            try (CSVWriter csv = createCsvWriter(baseName)) {
                XMLStreamReader streamReader = createXmlReader();
                while (streamReader.hasNext()) {
                    streamReader.next();
                    if (!isElementRowStart(streamReader)) continue;

                    row++;
                    line = updateMapping(streamReader, mappingProvided, mapping, line, row);
                    collectAttributeValues(streamReader, mapping, line, row);
                    callback.forRow(row, streamReader);
                    csv.writeNext(line);
                }
                streamReader.close();
            }
            if (isMappingProvided())
                writeHeader(baseName, names);
            else
                writeHeader(baseName, mapping);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.out.println("Done processing "+file+" with " + row + " rows in " + (System.currentTimeMillis() - start)/1000 + " seconds.");
            callback.end();
        }
    }

    private boolean isElementRowStart(XMLStreamReader streamReader) {
        return START_ELEMENT == streamReader.getEventType() && "row".equals(streamReader.getLocalName());
    }

    private String[] updateMapping(XMLStreamReader streamReader, boolean mappingProvided, Map<String, Integer> mapping, String[] line, int count) {
        if (!mappingProvided && sampleMappingUpdated(streamReader, mapping, count)) {
            line = new String[mapping.size()];
        }
        return line;
    }

    private void collectAttributeValues(XMLStreamReader streamReader, Map<String, Integer> mapping, String[] line, int row) {
        Arrays.fill(line, null);
        int attributeCount = streamReader.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            String attrName = streamReader.getAttributeLocalName(i);
            Integer index = mapping.get(attrName);
            if (index == null) continue; // missing mapping
            String value = streamReader.getAttributeValue(i);
            if (shouldOutputValue(index, attrName, value, row)) line[index] = cleanUp(index, attrName, value, row);
        }
    }

    private String cleanUp(int index, String attrName, String value, int row) {
        return value.replaceAll(CLEANUP_REGEXP,"");
    }

    private boolean shouldOutputValue(int index, String attrName, String value, int row) {
        return value.length() <= SKIP_TEXT_SIZE;
    }

    private boolean sampleMappingUpdated(XMLStreamReader streamReader, Map<String, Integer> mapping, int count) {
        boolean changed = false;
        if (count < SAMPLE_ROWS) {
            int attributeCount = streamReader.getAttributeCount();
            for (int i = 0; i < attributeCount; i++) {
                String attrName = streamReader.getAttributeLocalName(i);
                if (!mapping.containsKey(attrName)) {
                    mapping.put(attrName, i);
                    changed = true;
                }
            }
        }
        return changed;
    }

    private boolean isMappingProvided() {
        return names != null && names.length > 0;
    }

    private XMLStreamReader createXmlReader() throws IOException, XMLStreamException {
        InputStream is = new FileInputStream(file);
        if (file.endsWith(".gz")) is = new GZIPInputStream(is);
        return FACTORY.createXMLStreamReader(new BufferedInputStream(is, MB));
    }

    public static CSVWriter createCsvWriter(String baseName) {
        try {
            return new CSVWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(baseName + ".csv.gz"), MB), "UTF-8"), ',', '"', '"');
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private Map<String, Integer> createMapping() {
        Map<String, Integer> mapping = new HashMap<>(5);
        if (names != null && names.length > 0) {
            for (int i = 0; i < names.length; i++) {
                mapping.put(names[i].split("#")[0], i);
            }
        }
        return mapping;
    }

    public static void writeHeader(String baseName, Map<String, Integer> mapping) throws IOException {
        String[] line = new String[mapping.size()];
        for (Map.Entry<String, Integer> entry : mapping.entrySet()) {
            line[entry.getValue()] = entry.getKey();
        }
        writeHeader(baseName, line);
    }

    public static void writeHeader(String baseName, String...fields) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(baseName + "_header.csv"), ',', '"', '"')) {
            String[] header = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                header[i] = field.contains("#") ? field.split("#")[1] : field;

            }
            writer.writeNext(header);
        }
    }
}
