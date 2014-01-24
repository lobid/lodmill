default files = FLUX_DIR;

files+"hbz01XmlClobs.tar.bz2"|
read-dir(recursive="true")|
open-file(compression="BZIP2") |
open-tar|
decode-xml |
xml-tee| {
        split-xml(entityname="ListRecords") |
        write-xml(encoding="utf8",filesuffix="",compression="bz2",startindex="2", endindex="7",target="/tmp/xml/",property="/OAI-PMH/ListRecords/record/metadata/record/datafield[@tag='001']/subfield[@code='a']")
        } {
        handle-mabxml |
        morph(files+"morph-hbz01-to-lobid.xml")|
        stream-tee | {
                encode-stats(filename="tmp.stats.csv")
        }{
                encode-ntriples |
                triples-to-rdfmodel(input="N-TRIPLE")|
                write-rdfmodel(property="http://purl.org/lobid/lv#hbzID",  serialization="N-TRIPLES",startindex="2", endindex="7",target="/tmp/nt/") 
        }
};