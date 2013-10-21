default files = FLUX_DIR;

files + "gndRdf.xml" |
open-file |
decode-xml|
split-xml(entityName="Description",toplevelelement="rdf:RDF")|
extract-literals|
triples-to-rdfmodel(input="RDF/XML")|
write-rdfmodel(serialization="N-TRIPLE",property="http://d-nb.info/standards/elementset/gnd#gndIdentifier",startindex="0",endindex="3",filesuffix="nt",target="tmp");