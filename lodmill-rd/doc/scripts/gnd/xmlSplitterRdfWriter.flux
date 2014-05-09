default files = FLUX_DIR;

"http://gnd-proxy.lobid.org/oai/repository" |
open-oaipmh(dateFrom="2014-04-24",dateUntil="2014-04-30",metadataPrefix="RDFxml",setSpec="authorities") |
decode-xml|
xml-tee | {
        split-xml(entityName="Description",toplevelelement="rdf:RDF")|
        extract-literals|
        triples-to-rdfmodel(input="RDF/XML",inferenceModel="gndo.ttl",propertyIdentifyingTheNodeForInferencing="http://d-nb.info/standards/elementset/gnd#gndIdentifier")|
        write-rdfmodel(serialization="N-TRIPLE",property="http://d-nb.info/standards/elementset/gnd#gndIdentifier",startindex="0",endindex="3",filesuffix="nt",target=target+"gnd_snapshot/")
}{
        split-xml(entityName="Description",toplevelelement="rdf:RDF")|
        write-xml(startindex="0", endindex="3",target=target+"xml/",property="/*[local-name() = 'RDF']/*[local-name() = 'Description']/*[local-name() = 'gndIdentifier']")
};
