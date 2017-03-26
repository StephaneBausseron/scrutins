#saxonb-xslt -s:maps/circonscriptions.svg -xsl:processors/circo.xsl -o:data/circo -ext:on
#saxonb-xslt -s:maps/communes.svg -xsl:processors/communes.xsl -o:data/communes -ext:on

SHELL=/bin/bash

SOURCE_DIR := processors
BUILD_DIR := output

# les fichiers et URLS dans le meme ordre
FILES := pres_2012.csv pres_2007.csv 2005.csv legi_2012.csv inseeinfos.csv
URLS := \
    https://www.data.gouv.fr/s/resources/election-presidentielle-2012-resultats-par-bureaux-de-vote-1/20150925-102751/PR12_Bvot_T1T2.txt \
    https://www.data.gouv.fr/s/resources/election-presidentielle-2007-resultats-par-bureaux-de-vote/20151001-154056/PR07_Bvot_T1T2.txt \
    https://www.data.gouv.fr/s/resources/referendum-de-2005-resultats-par-bureaux-de-vote/20150925-112918/RF05_BVot.txt \
    https://www.data.gouv.fr/s/resources/elections-legislatives-2012-resultats-par-bureaux-de-vote/20150925-103435/LG12_Bvot_T1T2.txt \
    https://public.opendatasoft.com/explore/dataset/correspondance-code-insee-code-postal/download/?format=csv\&timezone=Europe/Berlin\&use_labels_for_header=true

RAW_FILES := $(addprefix raw/,$(FILES))
DATA_FILES := $(addprefix data/,$(FILES))

PROCESSORS := $(wildcard processors/*.xsl)
SVG_FILES := $(PROCESSORS:processors/%.xsl=output/%.svg)
PNG_FILES := $(SVG_FILES:.svg=.png)

.PHONY: maps download

all: download maps

maps: output/non_2005.png output/hollande.png

$(PNG_FILES) $(SVG_FILES): | output

$(PNG_FILES): %.png: %.svg
	inkscape -f $< -e "$@"

$(SVG_FILES): output/%.svg: processors/%.xsl maps/communes.svg communes.xml
	saxonb-xslt -s:maps/communes.svg -xsl:$< -o:$@ -ext:on

output:
	mkdir output

communes.xml: communes.json
	basex -q "let \$$file := \"communes.json\" return json-to-xml(file:read-text(\$$file))" > communes.xml

communes.json: $(DATA_FILES)
	python get_election_data.py

$(DATA_FILES): | data

$(DATA_FILES): data/%.csv: raw/%.csv
	cp $< $@

# override previous rule
data/2005.csv: raw/2005.csv
	# les numeros de bureau de vote de Caen ont un probleme
	sed 's/Caen;\([0-9]\{2\}\);\([0-9]\);/Caen;\1\2;/' $< > $@

data:
	mkdir data

$(RAW_FILES): download

download:
	mkdir -p raw
	files=( $(RAW_FILES) ); urls=( $(URLS) ); \
	for i in $${!files[@]}; do \
	curl -o $${files[$$i]} -z $${files[$$i]} $${urls[$$i]}; \
	done

clean:
	rm -f communes.xml communes.json $(RAW_FILES) $(DATA_FILES) $(SVG_FILES) $(PNG_FILES)
