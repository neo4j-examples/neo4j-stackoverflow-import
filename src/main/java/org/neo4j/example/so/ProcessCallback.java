package org.neo4j.example.so;

import javax.xml.stream.XMLStreamReader;

/**
 * @author mh
 * @since 13.10.15
 */
public interface ProcessCallback {
    ProcessCallback NONE = new ProcessCallback() {
        public void start(String fileBaseName) { }
        public void forRow(int row, XMLStreamReader xmlStreamReader) { }
        public void end() { }
    };
    void start(String fileBaseName);
    void forRow(int row, XMLStreamReader xmlStreamReader);
    void end();
}
