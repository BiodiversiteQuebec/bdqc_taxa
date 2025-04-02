from bdqc_taxa import global_names
from unittest import TestCase
import context

class TestGlobalNames(TestCase):
    def test_verify(self, name = 'Acer saccharum'):
        result: list = global_names.verify(name)
        # Assert that the result is a dict with keys 'metadata' and 'names'
        self.assertIsInstance(result, dict)
        self.assertTrue('metadata' in result.keys())
        self.assertTrue('names' in result.keys())

        self.assertTrue(result['names'][0]['matchType'] == 'Exact')
        self.assertIsInstance(result['names'], list)
        [self.assertTrue(bool(v)) for k, v in result['names'][0].items()]

        # Assert that the resulted names contains at least 3 names
        self.assertTrue(len(result['names'][0]['results']) >= 3)

    def test_verify_no_result(self, name = 'Vincent Beauregard'):
        result: list = global_names.verify(name)
        self.assertIsInstance(result['names'], list)
        self.assertTrue(result['names'][0]['matchType'] == 'NoMatch')

    def test_solve_conflicts(self):
        gn_out: list = global_names._verify('Diptera')
        filtered_out = global_names.verify('Diptera')
        self.assertTrue(len(gn_out['names'][0]['results']) > len(filtered_out['names'][0]['results']))
        
    def test_trillium_authorship_conflict(self, name='Trillium erythrocarpum', authorship='Michx.'):
        result: list = global_names.verify(name, authorship)
        result = result['names'][0]['results']
        data_source_ids = [item['dataSourceId'] for item in result]
        # Assert that the length of unique data_source_ids == the length of result
        # This means that there are only one entry per data sources which is what we expect
        self.assertTrue(len(set(data_source_ids)) == len(result))

        
