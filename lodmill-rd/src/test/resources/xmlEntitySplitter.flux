default files = FLUX_DIR;

files + "gndRdf.xml" |
open-file |
decode-xml|
split-xml(entityName="Description")|
extract-literals|
write("stdout");
