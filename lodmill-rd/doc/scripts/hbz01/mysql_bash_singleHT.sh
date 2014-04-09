#!/bin/bash
# little helper to get a resource as ntriple record residing in mysql db
mysql --reconnect --quick --silent --raw --user=debian-sys-maint --password=tzSblDEUGC1XhJB7 <<EOQ
use lobid;
select data
  FROM resources where identifier IN ("$1") LIMIT 10;
EOQ
