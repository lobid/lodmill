#!/bin/bash
mysql --reconnect --quick --silent --raw --user=debian-sys-maint --password=tzSblDEUGC1XhJB7   <<EOQ
use lobid;
select data
 FROM resources$1;
EOQ
