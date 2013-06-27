default files = FLUX_DIR;

url+"" |
open-http-with-accept-header("Accept:text/plain") |
stream-to-string |
decode-ntriples |
morph(files + "morph-lobid-to-oaidc.xml") |
encode-oai-dc | write(""+out) ;