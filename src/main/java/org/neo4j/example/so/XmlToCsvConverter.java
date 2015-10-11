package org.neo4j.example.so;

import au.com.bytecode.opencsv.CSVWriter;

import javax.xml.bind.SchemaOutputResolver;
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
    private static final int SKIP_TEXT_SIZE = 255;

    private static final ExecutorService pool = Executors.newFixedThreadPool(4);
    private static final XMLInputFactory FACTORY = XMLInputFactory.newInstance();
    private static final int SAMPLE_ROWS = 100;

    private final String file;
    private final String[] names;

    public XmlToCsvConverter(String file, String[] names) {

        this.file = file;
        this.names = names;
    }

    public static void main(String[] args) throws IOException, XMLStreamException, InterruptedException {
        for (String arg : args) {
            String[] parts = arg.split(":");
            String[] names = (parts.length > 1) ? parts[1].split(",") : null;
            pool.execute(new XmlToCsvConverter(parts[0], names));
        }
        pool.shutdown();
        pool.awaitTermination(1000, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        String baseName = file.substring(0, file.indexOf('.'));
        System.out.println("Processing "+file);
        boolean mappingProvided = isMappingProvided();
        Map<String, Integer> mapping = createMapping();
        String[] line = new String[mapping.size()];

        int count = 0;
        long start = System.currentTimeMillis();
        try {
            try (CSVWriter csv = createCsvWriter(baseName)) {
                XMLStreamReader streamReader = createXmlReader();
                while (streamReader.hasNext()) {
                    streamReader.next();
                    if (!isElementRowStart(streamReader)) continue;

                    count++;
                    line = updateMapping(streamReader, mappingProvided, mapping, line, count);
                    collectAttributeValues(streamReader, mapping, line);
                    csv.writeNext(line);
                }
                streamReader.close();
            }
            writeHeader(baseName, mapping);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.out.println("Done processing "+file+" with " + count + " rows in " + (System.currentTimeMillis() - start)/1000 + " seconds.");

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

    private void collectAttributeValues(XMLStreamReader streamReader, Map<String, Integer> mapping, String[] line) {
        Arrays.fill(line, null);
        int attributeCount = streamReader.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            String attrName = streamReader.getAttributeLocalName(i);
            Integer index = mapping.get(attrName);
            if (index == null) continue; // missing mapping
            String value = streamReader.getAttributeValue(i);
            if (shouldOutputValue(index, attrName, value)) line[index] = value;
        }
    }

    private boolean shouldOutputValue(int index, String attrName, String value) {
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

    private CSVWriter createCsvWriter(String baseName) throws IOException {
        return new CSVWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(baseName + ".csv.gz"), MB), "UTF-8"), ',', '"', '\\');
    }

    private Map<String, Integer> createMapping() {
        Map<String, Integer> mapping = new HashMap<>(5);
        if (names != null && names.length > 0) {
            for (int i = 0; i < names.length; i++) {
                mapping.put(names[i], i);
            }
        }
        return mapping;
    }

    private void writeHeader(String baseName, Map<String, Integer> mapping) throws IOException {
        try (CSVWriter header = new CSVWriter(new FileWriter(baseName + "_header.csv"), ',', '"', '\\')) {
            String[] line = new String[mapping.size()];
            for (Map.Entry<String, Integer> entry : mapping.entrySet()) {
                line[entry.getValue()] = entry.getKey();
            }
            header.writeNext(line);
        }
    }
}
