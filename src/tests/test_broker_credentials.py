from unittest import TestCase
from unittest.mock import patch
from catenae import Link


class BrokerCredentialsTest(TestCase):
    def test_credentials_are_forwarded_and_ignored_values_are_not_logged(self):
        with patch('catenae.catenae.stopover.Stopover') as client, patch('catenae.catenae.Logger') as logger:
            Link(endpoint='http://fixture.invalid', enable_health=False,
                 client_id='fixture', client_secret='private-credential',
                 unknown_option='private-ignored-value')
        self.assertEqual(client.call_args.kwargs['client_id'], 'fixture')
        self.assertEqual(client.call_args.kwargs['client_secret'], 'private-credential')
        self.assertEqual(client.call_args.kwargs['allowed_pickle_classes'], ())
        self.assertNotIn('private-credential', str(logger.mock_calls))
        self.assertNotIn('private-ignored-value', str(logger.mock_calls))
