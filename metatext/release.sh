#!/bin/sh
scp -r org.lobid.metatext.p2/target/repository/* lobid@emphytos.hbz-nrw.de:/usr/local/lobid/src/lobid.org/download/tools/p2
scp org.lobid.metatext.p2/target/products/*.zip lobid@emphytos.hbz-nrw.de:/usr/local/lobid/src/lobid.org/download/tools
