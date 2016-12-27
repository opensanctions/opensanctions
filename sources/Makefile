
all: scrape parse

scrape: au_sanctions.scrape ch_seco_sanctions.scrape eu_eeas_sanctions.scrape \
	gb_hmt_sanctionslist.scrape zz_every_politician.scrape zz_interpol.scrape \
	zz_wb_debarred.scrape ua_sdfm_blacklist.scrape zz_unsc_sanctions.scrape \
	us_bis_denied.scrape us_cia_world_leaders.scrape us_ofac.scrape

parse: au_sanctions.parse ch_seco_sanctions.parse eu_eeas_sanctions.parse \
	gb_hmt_sanctionslist.parse zz_every_politician.parse zz_interpol.parse \
	zz_wb_debarred.parse ua_sdfm_blacklist.parse zz_unsc_sanctions.parse \
	us_bis_denied.parse us_cia_world_leaders.parse us_ofac.parse

%.scrape:
	make -C $(basename $@) scrape

%.parse:
	make -C $(basename $@) parse
