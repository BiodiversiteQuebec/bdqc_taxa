import pandas as pd
from gbif_insert.sql import Sql
from gbif_insert.gbif import Gbif
from gbif_insert.cache import clear_cache

#taxa_dict = {"GBIF Backbone Taxonomy" : 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c',
#             "Catalogue of Life" : '7ddf754f-d193-4cc9-b351-99906754a03b',
#             "ITIS" : '9ca92552-f23a-41a8-a140-01abaa31c931',
#             "VASCAN" : '3f8a1297-3259-4700-91fc-acc4170b27ce',
#             "Bryoquel" : "e2178209-373b-4370-9ef4-f0b4bc964b40",
#             "CDPNQ" : "9b779078-1fd1-4492-8bbe-0892b0d13192"
#             }

clear_cache()

taxa_sources = pd.DataFrame({
    'datasetKey': [
        'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c',
        '7ddf754f-d193-4cc9-b351-99906754a03b',
        '9ca92552-f23a-41a8-a140-01abaa31c931',
        '3f8a1297-3259-4700-91fc-acc4170b27ce',
        'e2178209-373b-4370-9ef4-f0b4bc964b40'
    ]
})

eml_xml_list, ds_uuid_list = Gbif.get_gbif_datasets(taxa_sources)

for eml_xml, ds_uuid in zip(eml_xml_list, ds_uuid_list):
    if not eml_xml:
        continue
    source_eml_url = f'https://api.gbif.org/v1/dataset/{ds_uuid}/document'
    Sql.insert_dataset(eml_xml, methods = None, data_type = 'taxa_checklist', source_eml_url = source_eml_url, source_alias = 'GBIF', shareable_data = True)
