-- Update level 1 groups
BEGIN;

-- Create new groups
INSERT INTO rubus.taxa_groups (short, vernacular_fr, vernacular_en, level) VALUES
('VASCULAR_PLANTS', 'Plantes vasculaires', 'Vascular plants', 1),
('NON_VASCULAR_PLANTS', 'Plantes non vasculaires', 'Non-vascular plants', 1),
('MOLLUSKS', 'Mollusques', 'Mollusks', 1)
('MICROORGANISMS', 'Microorganismes', 'Microorganisms', 1);


-- Reassign old groups to new groups
ALTER TABLE rubus.taxa_group_members
SET short = 'MICROORGANISMS'
WHERE short = 'OTHER_TAXONS';

ALTER TABLE rubus.taxa_group_members
SET short = 'MOLLUSKS'
WHERE id_taxa_obs IN (
    SELECT tgm.id_taxa_obs
    FROM rubus.taxa_group_members tgm
    LEFT JOIN taxa_obs ON tgm.id_taxa_obs = taxa_obs.id
    WHERE taxa_obs.scientific_name = 'Mollusca'
);

ALTER TABLE rubus.taxa_group_members
SET short = 'VASCULAR_PLANTS'
WHERE short IN ('ANGIOSPERMS', 'CONIFERS'
                'VASCULAR_CRYPTOGAM' 'OTHER_GYMNOSPERMS');

ALTER TABLE rubus.taxa_group_members
SET short = 'NON_VASCULAR_PLANTS'
WHERE short = 'BRYOPHYTES'
    OR id_taxa_obs IN (
        SELECT tgm.id_taxa_obs
        FROM rubus.taxa_group_members tgm
        LEFT JOIN taxa_obs ON tgm.id_taxa_obs = taxa_obs.id
        WHERE taxa_obs.scientific_name IN ('Anthocerotophyta', 'Marchantiophyta')
    );

ALTER TABLE rubus.taxa_group_members
SET short = 'ALGAE'
WHERE id_taxa_obs IN (
    SELECT tgm.id_taxa_obs
    FROM rubus.taxa_group_members tgm
    LEFT JOIN taxa_obs ON tgm.id_taxa_obs = taxa_obs.id
    WHERE taxa_obs.scientific_name = 'Glaucophyta'
);

ALTER TABLE rubus.taxa_group_members
SET short = 'OTHER_INVERTEBRATES'
WHERE short IN ('TUNICATES', 'LANCELETS');

-- Delete the old groups
DELETE FROM rubus.taxa_groups WHERE short IN ('TUNICATES', 'LANCELETS', 'ANGIOSPERMS', 'CONIFERS', 'VASCULAR_CRYPTOGAM',
'OTHER_GYMNOSPERMS', 'BRYOPHYTES', 'OTHER_PLANTS', 'OTHER_TAXONS', 'CDPNQ_EMV');

COMMIT;


-- Update rubus.taxa_view and api.taxa
-- to include new level 2 group status
BEGIN;

INSERT INTO rubus.taxa_groups (short, vernacular_fr, vernacular_en, level) VALUES
('SARA_ENDANGERED', 'En voie de disparition', 'Endangered', 2),
('SARA_THREATENED', 'Menacée', 'Threatened', 2),
('SARA_SPECIAL_CONCERN', 'Préoccupante', 'Special Concern', 2);

-- Run group_members_update.R script to insert/update
-- SARA status and NatureServe S ranks

COMMIT;