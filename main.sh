#!/bin/bash -ex
COLLECTOR=~/tool/DAP-FTSCollector
cd ~
rm -f ~/ocha.db
python $COLLECTOR/metadata/metadata.py
python $COLLECTOR/ckan_loading/generate_chd_indicators.py
$COLLECTOR/archives/archive
echo done

