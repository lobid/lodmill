default files = FLUX_DIR;

"/files/open_data/closed/gnd/201402/2014-02-28.rdf" |
open-file |
decode-xml|
split-xml(entityName="Description",toplevelelement="rdf:RDF")|
extract-literals|
triples-to-rdfmodel(input="RDF/XML")|
write-rdfmodel(serialization="N-TRIPLE",property="http://d-nb.info/standards/elementset/gnd#gndIdentifier",startindex="0",endindex="3",filesuffix="nt",target="/files/open_data/closed/gnd/gnd_snapshot/");
