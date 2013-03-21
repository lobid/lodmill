default files = FLUX_DIR;

files + "zvdd_print_and_digital-test-set.xml" |
open-file |
decode-xml |
handle-marcxml |
morph(files + "../mapping/morph-zvdd_title-digital-rdfld.xml") |
encode-dot |
write(files + "morph-zvdd_title-digital-rdfld.dot")
;