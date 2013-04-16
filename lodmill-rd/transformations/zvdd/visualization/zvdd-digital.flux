default files = FLUX_DIR;

files + "zvdd-print-and-digital-test-set.xml" |
open-file |
decode-xml |
handle-marcxml |
morph(files + "../mapping/morph_zvdd-title-digital2ld.xml") |
encode-dot |
write(files + "morph_zvdd-title-digital2ld.dot")
;