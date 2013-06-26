default files = FLUX_DIR;

url+"" |
open-http-cn |
stream-to-string |
decode-ntriple |
morph(files + "morph-lobid-to-oaidc.xml") |
encode-xml | write(""+out) ;