// Read from elasticsearch index, get the "source" field assuming there
// resides an XML document, unwrap it (get rid of xml declaration and root
// element) and concatenate the xml records into a big ziped one, wrapped
// in one xml declaration and one root element.

// Some statistics :
// - works on one elasticsearch node
// - adding 1/8 load to a 8 core CPU
// - performs at ~1k docs/s
// - results in a 7 GB file with 18M records
// - 7 GB file unpacks to ~80 GB

default files = FLUX_DIR;

"" |
read-elasticsearch(clustername="quaoar", hostname="193.30.112.172", indexname="hbz01", batchsize="1000", shards="0,1,2,3,4") |
decode-xml|
split-xml(entityname="record",xmlDeclaration="")|
extract-literals|
	write("/files/open_data/closed/hbz01MabXmlClobsFulldump.xml.gz", compression="gzip", header="<?xml version = \"1.0\" encoding = \"UTF-8\"?><ListRecords>",footer="</ListRecords>" )
;
