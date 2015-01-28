// A simple transformation workflow.
// Gives some statistics and writes single files as N-TRIPLES to ./tmp/ .

input_filename="/tmp/acc-Mon-0041/acc-Mon-0041.tar.gz"
default files = FLUX_DIR;

input_filename|
open-file |
open-tar|
decode-xml |
handle-mabxml |
morph(files+"morph-hbz01-to-lobid.xml") |
stream-tee | {
	encode-stats(filename="tmp.stats.csv")
}{
	encode-ntriples |
	triples-to-rdfmodel(input="N-TRIPLE") |
	write-rdfmodel(target="tmp", startindex="0", endIndex="2", property="http://purl.org/lobid/lv#hbzID")
};
