// Builds a mysql table with two colums (see morph): 
// the first is the name , the second the geonames ID.
// Only data which belongs to Germany/Nrw is considered.
 
default files = FLUX_DIR;

files + "geonames_DE_sample.csv" |
open-file |
as-lines|
decode-csv("\t")|
morph(files+"morph-geonamesCsvDE-nrwMysqlRow.xml")|
write-mysql(dbname="lobid",dbProtocolAndAdress="jdbc:mysql://localhost/",Password="tzSblDEUGC1XhJB7",Tablename="NrwPlacesGeonamesId",Username="debian-sys-maint") ;
