BEGIN;
SET ROLE coleo;

DELETE FROM rubus.taxa_group_members
WHERE id_taxa_obs in (
    3818770, -- Desmognathus ochrophaeus
    3818794, -- Acer nigrum
    970120, -- Calypso bulbosa
    1167163, -- Cirsium scariosum
    3818791, -- Cypripedium passerinum
    3818797, -- Geranium maculatum
    3818799, -- Goodyera pubescens
    3818808, -- Platanthera macrophylla
    3818807, -- Platanus occidentalis
    3818809, -- Pterospora andromedea
    3818805, -- Ulmus thomasii
    3818811, -- Woodsia obtusa obtusa
    3818775, -- Lasiurus cinereus (Aeorestes cinereus)
    3818774, -- Lasionycteris noctivagans
    3818778, -- Lasiurus borealis
    3818786, -- Lanius ludovicianus
    3818785, -- Melanerpes erythrocephalus
    3818812, -- Acipenser oxyrinchus
    1953296, -- Clemmys guttata
    3818821, -- Graptemys geographica
    3818823 -- Sternotherus odoratus
)
AND short = 'SENSITIVE';

ROLLBACK;