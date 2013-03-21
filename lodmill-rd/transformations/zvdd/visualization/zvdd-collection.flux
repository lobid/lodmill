default files = FLUX_DIR;

files + "zvdd_collections-test-set.xml" |
open-file |
decode-xml |
handle-marcxml |
morph(files + "../mapping/morph-zvdd_collection-rdfld.xml") |
encode-dot  |
write(files + "morph-zvdd_collection-rdfld.dot")
;