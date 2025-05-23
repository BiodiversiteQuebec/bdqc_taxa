-- DROP TABLE IF EXISTS rubus.taxa_groups;
CREATE TABLE IF NOT EXISTS rubus.taxa_groups (
    id serial primary key,
    short varchar(20),
    vernacular_fr text,
    vernacular_en text,
    level integer,
    source_desc text,
    groups_within text[]
);

ALTER TABLE IF EXISTS rubus.taxa_groups
    OWNER to coleo;

CREATE INDEX IF NOT EXISTS taxa_groups_short_idx ON rubus.taxa_groups (short);
-- Create unique index on short name
CREATE UNIQUE INDEX IF NOT EXISTS taxa_groups_short_unique_idx ON rubus.taxa_groups (short);

ALTER TABLE rubus.taxa_groups OWNER TO coleo;
ALTER INDEX rubus.taxa_groups_short_idx OWNER TO coleo;
ALTER INDEX rubus.taxa_groups_short_unique_idx OWNER TO coleo;

GRANT INSERT, REFERENCES, SELECT, TRIGGER, TRUNCATE, UPDATE ON TABLE rubus.taxa_groups TO read_write_all;
GRANT REFERENCES, SELECT, TRIGGER ON TABLE rubus.taxa_groups TO read_only_all;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

--
-- Data for Name: taxa_groups; Type: TABLE DATA; Schema: rubus; Owner: coleo
--

-- DESCRIPTION OF LEVELS
-- 0: All quebec taxa, members are gathered from the observations within_quebec
-- 1: High level groups, Contains exclusive taxas to other level 1 groups
-- 2: Application level groups defined by scientific_name, From specific list for specific analysis, may overlaps with other groups
-- 3: Application level groups defined by other groups instead of scientific_name

COPY taxa_groups (short, id, vernacular_fr, vernacular_en, level, source_desc) FROM stdin;
AMPHIBIANS	1	Amphibiens	Amphibians	1	NULL
BIRDS	2	Oiseaux	Birds	1	NULL
MAMMALS	3	Mammifères	Mammals	1	NULL
REPTILES	4	Reptiles	Reptiles	1	NULL
FISH	5	Poissons	Fish	1	NULL
TUNICATES	6	Tuniciers	Tunicates	1	NULL
LANCELETS	7	Céphalocordés	Lancelets	1	NULL
ARTHROPODS	8	Arthropodes	Arthropods	1	NULL
OTHER_INVERTEBRATES	9	Autres invertébrés	Other invertebrates	1	NULL
OTHER_TAXONS	10	Autres taxons	Other taxons	1	NULL
FUNGI	11	Mycètes	Fungi	1	NULL
ANGIOSPERMS	12	Angiospermes	Angiosperms	1	NULL
CONIFERS	13	Conifères	Conifers	1	NULL
VASCULAR_CRYPTOGAM	14	Cryptogames vasculaires	Vascular cryptogam	1	NULL
OTHER_GYMNOSPERMS	15	Autres gymnospermes	Other gymnosperms	1	NULL
ALGAE	16	Algues	Algae	1	NULL
BRYOPHYTES	17	Bryophytes	Bryophytes	1	NULL
OTHER_PLANTS	18	Autres plantes	Other plants	1	NULL
ALL_SPECIES	19	Toutes les espèces	All species	0	NULL
INVASIVE_SPECIES	25	Espèce envahissante	Invasive species	2	Sentinelle, Agriculture, environnement et ressources naturelles Québec
SENTINELLE_INVASIVE 33    Espèce exotique envahissante	Exotic invasive species 2	SENTINELLE
PRINCIPAL_INVASIVE 34    Principales espèces exotiques envahissantes	NULL 2	Agriculture, environnement et ressources naturelles Québec
CDPNQ_SUSC	21	Espèce susceptible		2	CDPNQ
CDPNQ_VUL	22	Espèce vulnérable		2	CDPNQ
CDPNQ_VUL_HARVEST	23	Espèce vulnérable à la récolte		2	CDPNQ
CDPNQ_ENDANGERED	24	Espèce menacée		2	CDPNQ
CDPNQ_S1	27	Rang S1	S1 Rank	2	CDPNQ
CDPNQ_S2	28	Rang S2	S2 Rank	2	CDPNQ
CDPNQ_S3	29	Rang S3	S3 Rank	2	CDPNQ
SENSITIVE	31	Espèce sensibles	Sensitive species	2	CDPNQ
CDPNQ_EMV    32	Espèces menacées, vulnérables ou susceptibles   At-risk species 2	CDPNQ
\.

INSERT INTO rubus.taxa_group_members (short, id, vernacular_fr, vernacular_en, level, groups_within)
VALUES
    -- ('CDPNQ_RISK', ARRAY['CDPNQ_S1', 'CDPNQ_S2', 'CDPNQ_S3']),
    -- ('CDPNQ_STATUS', ARRAY['CDPNQ_SUSC', 'CDPNQ_VUL', 'CDPNQ_VUL_HARVEST', 'CDPNQ_ENDANGERED']);
    ('CDPNQ_RISK', 30, 'En situation précaire', 'At risk', 3, ARRAY['CDPNQ_S1', 'CDPNQ_S2', 'CDPNQ_S3']),
    ('CDPNQ_STATUS', 26, 'Espèces à statut CDPNQ', 'Species at risk ', 3, ARRAY['CDPNQ_SUSC', 'CDPNQ_VUL', 'CDPNQ_VUL_HARVEST', 'CDPNQ_ENDANGERED']);

--------------------------------------------------------------------------
--------------------------------------------------------------------------

--
-- Data for Name: taxa_group_members; Type: TABLE DATA; Schema: rubus; Owner: coleo
--

-- DROP TABLE IF EXISTS rubus.taxa_group_members CASCADE;
CREATE TABLE rubus.taxa_group_members (
    short varchar(20),
    scientific_name text,
    id_taxa_obs NOT NULL
);

ALTER TABLE IF EXISTS rubus.taxa_group_members
    OWNER to coleo;

ALTER TABLE rubus.taxa_group_members
    ADD COLUMN IF NOT EXISTS id_taxa_obs integer
    REFERENCES rubus.taxa_obs(id) ON DELETE SET NULL;

-- Not null constraint on id_taxa_obs
ALTER TABLE rubus.taxa_group_members
    ALTER COLUMN id_taxa_obs SET NOT NULL;

-- Attempt to drop the column with CASCADE to remove all dependencies
ALTER TABLE rubus.taxa_group_members
    DROP COLUMN IF EXISTS scientific_name CASCADE;

CREATE INDEX IF NOT EXISTS taxa_group_members_id_taxa_obs_idx
    ON rubus.taxa_group_members (id_taxa_obs);

CREATE UNIQUE INDEX IF NOT EXISTS idx_taxa_group_members_short_id_taxa_obs
    ON rubus.taxa_group_members (short, id_taxa_obs);

ALTER TABLE rubus.taxa_group_members OWNER TO coleo;
ALTER INDEX rubus.taxa_group_members_id_taxa_obs_idx OWNER TO coleo;
ALTER INDEX rubus.idx_taxa_group_members_short_id_taxa_obs OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- Fonction d'injection et de relation
CREATE OR REPLACE FUNCTION rubus.insert_taxa_obs_group_member(
    short_group char(255),
    scientific_name text,
    authorship text DEFAULT '',
    rank text DEFAULT '',
    parent_scientific_name text DEFAULT '')
RETURNS void AS $$ 
DECLARE
    taxa_obs_id integer;
BEGIN
    -- 3.1. Insérer dans taxa_obs si non existant
    INSERT INTO public.taxa_obs (scientific_name, authorship, rank, parent_scientific_name)
    VALUES ($2, $3, $4, $5)
    ON CONFLICT DO NOTHING;

    -- 3.2. Récupérer l'id du taxa_obs
    SELECT id INTO taxa_obs_id FROM public.taxa_obs t
      WHERE t.scientific_name = $2
        AND COALESCE(t.authorship, '') = $3
        AND COALESCE(t.rank, '') = $4
        AND COALESCE(t.parent_scientific_name, '') = $5
      LIMIT 1;

    -- 3.3. Insérer dans taxa_group_members avec id_taxa_obs
    INSERT INTO rubus.taxa_group_members (short, id_taxa_obs)
    VALUES ($1, taxa_obs_id)
    ON CONFLICT DO NOTHING;

    -- 3.4. Rafraîchir taxa_ref pour ce taxa_obs
    BEGIN
        PERFORM rubus.insert_taxa_ref_from_taxa_obs(taxa_obs_id, $2, $3, $5);
    EXCEPTION
        WHEN OTHERS THEN
        RAISE NOTICE 'Error inserting record with id % and scientific name %', taxa_obs_id, $2;
    END;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION rubus.insert_taxa_obs_group_member(char(255), text, text, text, text) OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DELETE FROM rubus.taxa_group_members;
COPY rubus.taxa_group_members (short, scientific_name) FROM stdin;
AMPHIBIANS	Amphibia
BIRDS	Aves
MAMMALS	Mammalia
REPTILES	Reptilia
FISH	Myxini
FISH	Holocephali
FISH	Actinopterygii
FISH	Cephalaspidomorphi
FISH	Elasmobranchii
FISH	Coelacanthiformes
TUNICATES	Ascidiacea
TUNICATES	Thaliacea
TUNICATES	Appendicularia
LANCELETS	Leptocardii
ARTHROPODS	Arthropoda
OTHER_INVERTEBRATES	Hemichordata
OTHER_INVERTEBRATES	Micrognathozoa
OTHER_INVERTEBRATES	Mollusca
OTHER_INVERTEBRATES	Myxozoa
OTHER_INVERTEBRATES	Nematoda
OTHER_INVERTEBRATES	Nematomorpha
OTHER_INVERTEBRATES	Nemertea
OTHER_INVERTEBRATES	Onychophora
OTHER_INVERTEBRATES	Orthonectida
OTHER_INVERTEBRATES	Phoronida
OTHER_INVERTEBRATES	Placozoa
OTHER_INVERTEBRATES	Platyhelminthes
OTHER_INVERTEBRATES	Porifera
OTHER_INVERTEBRATES	Rotifera
OTHER_INVERTEBRATES	Sipuncula
OTHER_INVERTEBRATES	Xenacoelomorpha
OTHER_INVERTEBRATES	Tardigrada
OTHER_INVERTEBRATES	Acanthocephala
OTHER_INVERTEBRATES	Annelida
OTHER_INVERTEBRATES	Brachiopoda
OTHER_INVERTEBRATES	Bryozoa
OTHER_INVERTEBRATES	Cephalorhyncha
OTHER_INVERTEBRATES	Chaetognatha
OTHER_INVERTEBRATES	Cnidaria
OTHER_INVERTEBRATES	Ctenophora
OTHER_INVERTEBRATES	Cycliophora
OTHER_INVERTEBRATES	Dicyemida
OTHER_INVERTEBRATES	Echinodermata
OTHER_INVERTEBRATES	Entoprocta
OTHER_INVERTEBRATES	Gastrotricha
OTHER_INVERTEBRATES	Gnathostomulida
OTHER_TAXONS	Protozoa
OTHER_TAXONS	Viruses
OTHER_TAXONS	Chromista
OTHER_TAXONS	Bacteria
OTHER_TAXONS	Archaea
FUNGI	Fungi
ANGIOSPERMS	Magnoliopsida
ANGIOSPERMS	Liliopsida
CONIFERS	Pinopsida
VASCULAR_CRYPTOGAM	Lycopodiopsida
VASCULAR_CRYPTOGAM	Polypodiopsida
OTHER_GYMNOSPERMS	Gnetopsida
OTHER_GYMNOSPERMS	Cycadopsida
OTHER_GYMNOSPERMS	Ginkgoopsida
ALGAE	Chlorophyta
ALGAE	Charophyta
ALGAE	Rhodophyta
BRYOPHYTES	Bryophyta
OTHER_PLANTS	Glaucophyta
OTHER_PLANTS	Anthocerotophyta
OTHER_PLANTS	Marchantiophyta
CDPNQ_S1	Apalone spinifera
CDPNQ_S1	Noturus insignis
CDPNQ_S1	Podiceps auritus
CDPNQ_S1	Charadrius melodus
CDPNQ_S1	Hydroprogne caspia
CDPNQ_S1	Sterna dougallii
CDPNQ_S1	Melanerpes erythrocephalus
CDPNQ_S1	Lanius ludovicianus
CDPNQ_S1	Setophaga cerulea
CDPNQ_S1	Parkesia motacilla
CDPNQ_S1	Centronyx henslowii
CDPNQ_S1	Calidris canutus rufa
CDPNQ_S1	Sorex dispar
CDPNQ_S1	Lasiurus borealis
CDPNQ_S1	Careproctus reinhardti
CDPNQ_S1	Coregonus artedi
CDPNQ_S1	Moxostoma hubbsi
CDPNQ_S1	Melanogrammus aeglefinus
CDPNQ_S1	Pollachius virens
CDPNQ_S1	Scomber scombrus
CDPNQ_S1	Gymnelus viridis
CDPNQ_S1	Lycodes esmarkii
CDPNQ_S1	Anarhichas minor
CDPNQ_S1	Cryptacanthodes maculatus
CDPNQ_S1	Lophius americanus
CDPNQ_S1	Sorex gaspensis
CDPNQ_S1	Myotis lucifugus
CDPNQ_S1	Myotis leibii
CDPNQ_S1	Myotis septentrionalis
CDPNQ_S1	Perimyotis subflavus
CDPNQ_S1	Hemitripterus americanus
CDPNQ_S1	Delphinapterus leucas
CDPNQ_S1	Balaenoptera musculus
CDPNQ_S1	Eubalaena glacialis
CDPNQ_S1	Gulo gulo
CDPNQ_S1	Phoca vitulina mellonae
CDPNQ_S1	Rangifer tarandus
CDPNQ_S1	Myoxocephalus thompsonii
CDPNQ_S2	Calidris pusilla
CDPNQ_S2	Limanda ferruginea
CDPNQ_S2	Chaetura pelagica
CDPNQ_S2	Progne subis
CDPNQ_S2	Cistothorus platensis
CDPNQ_S2	Catharus bicknelli
CDPNQ_S2	Vermivora cyanoptera
CDPNQ_S2	Vermivora chrysoptera
CDPNQ_S2	Ammodramus savannarum
CDPNQ_S2	Anguilla rostrata
CDPNQ_S2	Hybognathus hankinsoni
CDPNQ_S2	Moxostoma carinatum
CDPNQ_S2	Rangifer tarandus
CDPNQ_S2	Sternotherus odoratus
CDPNQ_S2	Podiceps grisegena
CDPNQ_S2	Chlidonias niger
CDPNQ_S2	Coccyzus americanus
CDPNQ_S2	Emydoidea blandingii
CDPNQ_S2	Delphinapterus leucas
CDPNQ_S2	Desmognathus ochrophaeus
CDPNQ_S2	Pseudacris triseriata
CDPNQ_S2	Pseudacris maculata
CDPNQ_S2	Ichthyomyzon fossor
CDPNQ_S2	Artediellus uncinatus
CDPNQ_S2	Icelus spatula
CDPNQ_S2	Liparis gibbus
CDPNQ_S2	Cyclopterus lumpus
CDPNQ_S2	Esox americanus vermiculatus
CDPNQ_S2	Boreogadus saida
CDPNQ_S2	Gadus ogac
CDPNQ_S2	Ammocrypta pellucida
CDPNQ_S2	Phalaropus tricolor
CDPNQ_S2	Glaucomys volans
CDPNQ_S2	Microtus pinetorum
CDPNQ_S2	Mustela nivalis
CDPNQ_S2	Storeria dekayi
CDPNQ_S2	Thamnophis saurita
CDPNQ_S2	Hydrobates leucorhous
CDPNQ_S2	Ixobrychus exilis
CDPNQ_S3	Pipilo erythrophthalmus
CDPNQ_S3	Spizella pusilla
CDPNQ_S3	Ammospiza nelsoni
CDPNQ_S3	Dolichonyx oryzivorus
CDPNQ_S3	Sturnella magna
CDPNQ_S3	Icterus spurius
CDPNQ_S3	Branta hutchinsii
CDPNQ_S3	Anser albifrons
CDPNQ_S3	Anser rossii
CDPNQ_S3	Branta bernicla
CDPNQ_S3	Mareca penelope
CDPNQ_S3	Aythya valisineria
CDPNQ_S3	Numenius phaeopus
CDPNQ_S3	Limosa haemastica
CDPNQ_S3	Arenaria interpres
CDPNQ_S3	Calidris bairdii
CDPNQ_S3	Calidris maritima
CDPNQ_S3	Calidris himantopus
CDPNQ_S3	Calidris subruficollis
CDPNQ_S3	Limnodromus scolopaceus
CDPNQ_S3	Hydrocoloeus minutus
CDPNQ_S3	Xema sabini
CDPNQ_S3	Pelecanus erythrorhynchos
CDPNQ_S3	Ursus maritimus
CDPNQ_S3	Phalacrocorax carbo
CDPNQ_S3	Somateria mollissima
CDPNQ_S3	Larus glaucoides
CDPNQ_S3	Megascops asio
CDPNQ_S3	Thryothorus ludovicianus
CDPNQ_S3	Ichthyomyzon unicuspis
CDPNQ_S3	Acipenser fulvescens
CDPNQ_S3	Acipenser oxyrinchus
CDPNQ_S3	Hiodon alosoides
CDPNQ_S3	Exoglossum maxillingua
CDPNQ_S3	Notropis rubellus
CDPNQ_S3	Carpiodes cyprinus
CDPNQ_S3	Microgadus tomcod
CDPNQ_S3	Brosme brosme
CDPNQ_S3	Didelphis virginiana
CDPNQ_S3	Dorosoma cepedianum
CDPNQ_S3	Butorides virescens
CDPNQ_S3	Nycticorax nycticorax
CDPNQ_S3	Spatula discors
CDPNQ_S3	Melanitta deglandi
CDPNQ_S3	Circus hudsonius
CDPNQ_S3	Buteo lagopus
CDPNQ_S3	Charadrius semipalmatus
CDPNQ_S3	Charadrius vociferus
CDPNQ_S3	Limnodromus griseus
CDPNQ_S3	Phalaropus lobatus
CDPNQ_S3	Chordeiles minor
CDPNQ_S3	Contopus cooperi
CDPNQ_S3	Eremophila alpestris
CDPNQ_S3	Petrochelidon pyrrhonota
CDPNQ_S3	Oporornis agilis
CDPNQ_S3	Pooecetes gramineus
CDPNQ_S3	Euphagus carolinus
CDPNQ_S3	Calidris melanotos
CDPNQ_S3	Calidris mauri
CDPNQ_S3	Gyrinophilus porphyriticus
CDPNQ_S3	Hemidactylium scutatum
CDPNQ_S3	Bucephala islandica
CDPNQ_S3	Melanerpes carolinus
CDPNQ_S3	Mola mola
CDPNQ_S3	Myoxocephalus scorpius
CDPNQ_S3	Gymnocanthus tricuspis
CDPNQ_S3	Leptagonus decagonus
CDPNQ_S3	Eumicrotremus spinosus
CDPNQ_S3	Alosa sapidissima
CDPNQ_S3	Esox americanus americanus
CDPNQ_S3	Esox niger
CDPNQ_S3	Notropis bifrenatus
CDPNQ_S3	Moxostoma valenciennesi
CDPNQ_S3	Noturus flavus
CDPNQ_S3	Ameiurus natalis
CDPNQ_S3	Phycis chesteri
CDPNQ_S3	Morone chrysops
CDPNQ_S3	Lepomis peltastes
CDPNQ_S3	Percina copelandi
CDPNQ_S3	Pholis gunnellus
CDPNQ_S3	Lycodes lavalaei
CDPNQ_S3	Lycodes vahlii
CDPNQ_S3	Eumesogrammus praecisus
CDPNQ_S3	Lumpenus lampretaeformis
CDPNQ_S3	Scophthalmus aquosus
CDPNQ_S3	Hippoglossus hippoglossus
CDPNQ_S3	Pleuronectes putnami
CDPNQ_S3	Pseudopleuronectes americanus
CDPNQ_S3	Centroscyllium fabricii
CDPNQ_S3	Lasionycteris noctivagans
CDPNQ_S3	Eptesicus fuscus
CDPNQ_S3	Lasiurus cinereus
CDPNQ_S3	Tamias minimus
CDPNQ_S3	Lagenorhynchus albirostris
CDPNQ_S3	Orcinus orca
CDPNQ_S3	Balaenoptera physalus
CDPNQ_S3	Megaptera novaeangliae
CDPNQ_S3	Rangifer tarandus
CDPNQ_S3	Glyptemys insculpta
CDPNQ_S3	Graptemys geographica
CDPNQ_S3	Lampropeltis triangulum
CDPNQ_S3	Nerodia sipedon
CDPNQ_S3	Osmerus mordax
CDPNQ_S3	Salvelinus alpinus oquassa
CDPNQ_S3	Gadus morhua
CDPNQ_S3	Ichthyomyzon castaneus
CDPNQ_S3	Gavia pacifica
CDPNQ_S3	Ardea alba
CDPNQ_S3	Cygnus buccinator
CDPNQ_S3	Somateria spectabilis
CDPNQ_S3	Histrionicus histrionicus
CDPNQ_S3	Oxyura jamaicensis
CDPNQ_S3	Aquila chrysaetos
CDPNQ_S3	Falco sparverius
CDPNQ_S3	Coturnicops noveboracensis
CDPNQ_S3	Pluvialis dominica
CDPNQ_S3	Tringa flavipes
CDPNQ_S3	Tringa semipalmata
CDPNQ_S3	Bartramia longicauda
CDPNQ_S3	Limosa fedoa
CDPNQ_S3	Calidris alpina
CDPNQ_S3	Stercorarius pomarinus
CDPNQ_S3	Stercorarius parasiticus
CDPNQ_S3	Stercorarius longicaudus
CDPNQ_S3	Chroicocephalus ridibundus
CDPNQ_S3	Fratercula arctica
CDPNQ_S3	Asio flammeus
CDPNQ_S3	Antrostomus vociferus
CDPNQ_S3	Contopus virens
CDPNQ_S3	Stelgidopteryx serripennis
CDPNQ_S3	Riparia riparia
CDPNQ_S3	Hirundo rustica
CDPNQ_S3	Polioptila caerulea
CDPNQ_S3	Hylocichla mustelina
CDPNQ_S3	Vireo flavifrons
SENSITIVE	Desmognathus ochrophaeus
SENSITIVE	Melanoplus gaspesiensis
SENSITIVE	Oeneis bore gaspeensis
SENSITIVE	Coenonympha nipisiquit
SENSITIVE	Lasionycteris noctivagans
SENSITIVE	Lasiurus cinereus
SENSITIVE	Myotis septentrionalis
SENSITIVE	Myotis leibii
SENSITIVE	Lasiurus borealis
SENSITIVE	Myotis lucifugus
SENSITIVE	Perimyotis subflavus
SENSITIVE	Margaritifera margaritifera
SENSITIVE	Aquila chrysaetos
SENSITIVE	Centronyx henslowii
SENSITIVE	Falco peregrinus
SENSITIVE	Melanerpes erythrocephalus
SENSITIVE	Lanius ludovicianus
SENSITIVE	Allium tricoccum
SENSITIVE	Aplectrum hyemale
SENSITIVE	Calypso bulbosa
SENSITIVE	Cirsium scariosum
SENSITIVE	Cypripedium passerinum
SENSITIVE	Cypripedium reginae
SENSITIVE	Cypripedium arietinum
SENSITIVE	Acer nigrum
SENSITIVE	Gentianopsis virgata macounii
SENSITIVE	Gentianopsis crinita
SENSITIVE	Geranium maculatum
SENSITIVE	Panax quinquefolius
SENSITIVE	Goodyera pubescens
SENSITIVE	Houstonia longifolia
SENSITIVE	Hydrophyllum canadense
SENSITIVE	Lobelia spicata
SENSITIVE	Monarda punctata
SENSITIVE	Galearis spectabilis
SENSITIVE	Ulmus thomasii
SENSITIVE	Phegopteris hexagonoptera
SENSITIVE	Platanus occidentalis
SENSITIVE	Platanthera macrophylla
SENSITIVE	Pterospora andromedea
SENSITIVE	Verbena simplex
SENSITIVE	Woodsia obtusa obtusa
SENSITIVE	Acipenser oxyrinchus
SENSITIVE	Acipenser fulvescens
SENSITIVE	Diadophis punctatus
SENSITIVE	Storeria dekayi
SENSITIVE	Nerodia sipedon
SENSITIVE	Opheodrys vernalis
SENSITIVE	Thamnophis saurita
SENSITIVE	Lampropeltis triangulum
SENSITIVE	Glyptemys insculpta
SENSITIVE	Graptemys geographica
SENSITIVE	Emydoidea blandingii
SENSITIVE	Sternotherus odoratus
SENSITIVE	Clemmys guttata
SENSITIVE	Apalone spinifera
\.

--------------------------------------------------------------------------
--------------------------------------------------------------------------

--
-- CREATE TAXA LOOKUP
--

-- DROP MATERIALIZED VIEW IF EXISTS rubus.taxa_obs_group_lookup CASCADE;
-- Dépendances
-- api.taxa
-- view api.taxa_groups
-- view atlas_api.observation_web_geom

CREATE TABLE rubus.taxa_obs_group_lookup (
    id_taxa_obs integer NOT NULL REFERENCES rubus.taxa_obs(id) ON DELETE CASCADE,
    id_group integer NOT NULL REFERENCES rubus.taxa_groups(id) ON DELETE CASCADE,
    short_group text NOT NULL REFERENCES rubus.taxa_groups(short) ON DELETE CASCADE
);

ALTER TABLE IF EXISTS rubus.taxa_obs_group_lookup
    OWNER to coleo;

CREATE UNIQUE INDEX idx_taxa_obs_group_lookup ON rubus.taxa_obs_group_lookup (id_taxa_obs, id_group, short_group);

CREATE INDEX IF NOT EXISTS taxa_obs_group_lookup_id_taxa_obs_idx
  ON rubus.taxa_obs_group_lookup (id_taxa_obs);

CREATE INDEX IF NOT EXISTS taxa_obs_group_lookup_id_group_idx
    ON rubus.taxa_obs_group_lookup (id_group);

CREATE INDEX IF NOT EXISTS taxa_obs_group_lookup_short_group_idx
    ON rubus.taxa_obs_group_lookup (short_group);

ALTER TABLE rubus.taxa_obs_group_lookup OWNER TO coleo;
ALTER INDEX rubus.taxa_obs_group_lookup_id_taxa_obs_idx OWNER TO coleo;
ALTER INDEX rubus.taxa_obs_group_lookup_id_group_idx OWNER TO coleo;
ALTER INDEX rubus.taxa_obs_group_lookup_short_group_idx OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- View 1: Level 1 and 2 groups
CREATE OR REPLACE VIEW rubus.taxa_obs_group_lookup_level_1_2_view AS
SELECT DISTINCT
    obs_lookup.id_taxa_obs, 
    taxa_groups.id AS id_group, 
    taxa_groups.short AS short_group
FROM rubus.taxa_group_members group_m
JOIN taxa_groups ON taxa_groups.short = group_m.short
JOIN taxa_obs_ref_lookup match_lu
    ON group_m.id_taxa_obs = match_lu.id_taxa_obs
    AND match_lu.is_parent IS FALSE
LEFT JOIN rubus.taxa_obs_ref_lookup obs_lookup
    ON match_lu.id_taxa_ref_valid = obs_lookup.id_taxa_ref
WHERE taxa_groups.level = ANY(ARRAY[1, 2]);

ALTER TABLE rubus.taxa_obs_group_lookup_level_1_2_view OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- View 2: Level 0 groups (Quebec observations)
CREATE OR REPLACE VIEW rubus.taxa_obs_group_lookup_quebec_view AS
SELECT DISTINCT ON (id_taxa_obs)
    id_taxa_obs, 
    taxa_groups.id AS id_group, 
    taxa_groups.short AS short_group
FROM observations_partitions.within_quebec, taxa_groups
WHERE level = 0;

ALTER TABLE rubus.taxa_obs_group_lookup_quebec_view OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- View 3: Level 3 groups
CREATE OR REPLACE VIEW rubus.taxa_obs_group_lookup_level_3_view AS
SELECT 
    level_1_2.id_taxa_obs, 
    level_3_groups.id AS id_group, 
    level_3_groups.short AS short_group
FROM taxa_groups AS level_3_groups
JOIN rubus.taxa_obs_group_lookup_level_1_2_view AS level_1_2
    ON level_1_2.short_group = ANY(level_3_groups.groups_within)
WHERE level_3_groups.level = 3;

ALTER TABLE rubus.taxa_obs_group_lookup_level_3_view OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION rubus.refresh_taxa_obs_group_lookup()
RETURNS void AS $$
BEGIN
    DELETE FROM rubus.taxa_obs_group_lookup;

    INSERT INTO rubus.taxa_obs_group_lookup (id_taxa_obs, id_group, short_group)
    SELECT * FROM rubus.taxa_obs_group_lookup_level_1_2_view
    UNION
    SELECT * FROM rubus.taxa_obs_group_lookup_quebec_view
    UNION
    SELECT * FROM rubus.taxa_obs_group_lookup_level_3_view
    ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION rubus.refresh_taxa_obs_group_lookup() OWNER TO coleo;