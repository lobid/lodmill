default files = FLUX_DIR;

in+ "" |
open-file | 
generic-xml("metadata")|
morph(files + "dipp-qdc-to-lobid.xml")|
encode-ntriples(subject=""+subject)|
write(out+"");

