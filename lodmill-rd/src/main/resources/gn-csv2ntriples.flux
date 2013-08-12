default files = FLUX_DIR;

files + "geonames_DE_sample.csv" |
open-file |
as-lines|
decode-csv("\t")|
morph(files+"morphGeonamesCsv2ld.xml") |
encode-ntriples |
write(files + "geonamesCsv.nt");