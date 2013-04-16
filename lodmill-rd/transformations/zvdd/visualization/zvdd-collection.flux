default files = FLUX_DIR;

files + "zvdd-collections-test-set.xml" |
open-file |
decode-xml |
handle-marcxml |
morph(files + "../mapping/morph_zvdd-collection2ld.xml") |
encode-dot  |
write(files + "morph_zvdd-collection2ld.dot")
;