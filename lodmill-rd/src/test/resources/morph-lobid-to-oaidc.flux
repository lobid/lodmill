default files = FLUX_DIR;

url+"" |
open-http-with-accept-header(accept="text/plain", encoding="utf-8") |
stream-to-string |
decode-ntriples |
morph(files + "morph-lobid-to-oaidc.xml") |
encode-oai-dc | write(""+out) ;