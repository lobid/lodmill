default files = FLUX_DIR;
 // the input files are not provided here, 
//  see https://github.com/lobid/lodmill/blob/master/lodmill-rd/doc/zvdd/harvestOAI_hbz_zvdd.sh
 //  how to obtain them
default input ="../../../../../"; 
files+input + "/zvdd.xml"|
open-file |
decode-xml |
handle-marcxml |
morph(files + "morph-zvdd_title-print-rdfld.xml") |
encode-ntriples |
write(files + "zvdd_title-print-output_trans.nt");