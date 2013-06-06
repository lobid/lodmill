default files = FLUX_DIR;

files + "Bibdat1303pp_sample1.xml" |
open-file |
decode-xml |
handle-picaxml |
morph("morph_zdb-isil-file-pica2ld.xml") |
encode-triples-enrich-lobid-organisation |
write(files + "zdb-isil-file2lobid-organisations.ttl");