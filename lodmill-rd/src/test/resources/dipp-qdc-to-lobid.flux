default files = FLUX_DIR;

in+ "" |
open-file | 
generic-xml("metadata")|
morph(files + "dipp-qdc-to-lobid.xml")|
encode-ntriples-with-subject-as-parameter(subject=""+subject)|
write(out+"");

