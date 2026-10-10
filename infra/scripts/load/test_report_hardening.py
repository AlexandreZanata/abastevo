#!/usr/bin/env python3
"""Raw evidence cannot be silently changed or promoted to replicated capacity."""
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from report_hardening import verify_campaign


class EvidenceTests(unittest.TestCase):
    def fixture(self, root):
        path = root / 'trial-1-steady.csv'
        with path.open('w', newline='') as file:
            writer=csv.writer(file)
            writer.writerow(['route','scheduled_ms','started_ms','completed_ms','status'])
            for i in range(100):
                writer.writerow(['text_national',i*1000,i*1000,i*1000+i+1,'200'])
        summary={'trial':1,'phase':'steady','requested_seconds':300,'observation_seconds':300,
                 'offered':100,'completed':100,'success':100,'errors':0,'drops':0,'missed':0,
                 'p95_qualified':True,'p95_budget_pass':True,'reconciled':True,
                 'routes':{'text_national':{'n':100,'p95_ms':95}}}
        manifest={'format':'owned-synthetic-test','transport':'test','parameters':{'rate':1},
                  'stopped':True,'reasons':['later host reserve stop'], 'summaries':[summary],
                  'hashes':{path.name:hashlib.sha256(path.read_bytes()).hexdigest()}}
        (root/'manifest.json').write_text(json.dumps(manifest))
        return manifest,path

    def test_completed_window_does_not_accept_interrupted_replicated_campaign(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);self.fixture(root)
            report=verify_campaign(root)
            self.assertEqual(len(report['completed_valid_trials']),1)
            self.assertFalse(report['campaign_qualified'])

    def test_tampered_raw_or_accounting_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);manifest,path=self.fixture(root)
            path.write_text(path.read_text()+'tampered\n')
            with self.assertRaisesRegex(ValueError,'checksum'):verify_campaign(root)
            manifest,path=self.fixture(root)
            manifest['summaries'][0]['success']=99
            (root/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError,'accounting'):verify_campaign(root)

    def test_flags_cannot_qualify_missing_samples_or_unhashed_trials(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);manifest,path=self.fixture(root)
            manifest['summaries'][0]['p95_budget_ms']=90
            (root/'manifest.json').write_text(json.dumps(manifest))
            self.assertEqual(verify_campaign(root)['completed_valid_trials'], [])
            summary=manifest['summaries'][0]
            summary['p95_budget_ms']=500
            for key in ('offered','completed','success'):summary[key]=99
            summary['routes']['text_national']['n']=99
            path.write_text('\n'.join(path.read_text().splitlines()[:-1])+'\n')
            manifest['hashes'][path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
            (root/'manifest.json').write_text(json.dumps(manifest))
            self.assertEqual(verify_campaign(root)['completed_valid_trials'], [])
            manifest['hashes']={}
            (root/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError,'unhashed'):verify_campaign(root)


if __name__=='__main__':unittest.main()
