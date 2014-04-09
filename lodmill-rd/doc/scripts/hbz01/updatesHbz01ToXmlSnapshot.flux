default files = FLUX_DIR;

"/home/data/demeter/alephxml/clobs/update/20140329_20140330.tar.bz2"|
open-file(compression="BZIP2") |
open-tar|
decode-xml |
	split-xml(entityname="ListRecords") |
	write-xml(encoding="utf8",filesuffix="",compression="bz2",startindex="2", endindex="7",target="/files/open_data/closed/hbzvk/snapshot",property="/OAI-PMH/ListRecords/record/metadata/record/datafield[@tag='001']/subfield[@code='a']")
;
