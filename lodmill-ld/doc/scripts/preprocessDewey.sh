#!/bin/bash
# filter to only get skos:prefLabel. Rewrite URI.

grep  'skos/core#prefLabel' /files/open_data/closed/dewey/dewey.nt  |sed -e 's#20../../about\...##g' | sort -u >  dewey_preprocessed.nt
