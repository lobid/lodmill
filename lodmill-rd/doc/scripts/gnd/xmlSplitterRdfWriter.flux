default files = FLUX_DIR;

"http://gnd-proxy.lobid.org/oai/repository" |
open-oaipmh(dateFrom="2014-03-27",dateUntil="2014-04-07",metadataPrefix="RDFxml",setSpec="authorities") |
decode-xml|
split-xml(entityName="Description",toplevelelement="rdf:RDF")|
extract-literals|
triples-to-rdfmodel(input="RDF/XML")|
write-rdfmodel(serialization="N-TRIPLE",property="http://d-nb.info/standards/elementset/gnd#gndIdentifier",startindex="0",endindex="3",filesuffix="nt",target="/files/open_data/closed/gnd/gnd_snapshot/");
