"""The SecretVault organization configuration"""

import os
from dotenv import load_dotenv

load_dotenv()

# Organization configuration
org_config = {
    "org_credentials": {
        "secret_key": os.getenv("NILLION_ORG_SECRET_KEY"),
        "org_did": os.getenv("NILLION_ORG_DID"),
    },
    "nodes": [
        {
            "url": "https://nildb-nx8v.nillion.network",
            "did": "did:nil:testnet:nillion1qfrl8nje3nvwh6cryj63mz2y6gsdptvn07nx8v",
        },
        {
            "url": "https://nildb-p3mx.nillion.network",
            "did": "did:nil:testnet:nillion1uak7fgsp69kzfhdd6lfqv69fnzh3lprg2mp3mx",
        },
        {
            "url": "https://nildb-rugk.nillion.network",
            "did": "did:nil:testnet:nillion1kfremrp2mryxrynx66etjl8s7wazxc3rssrugk",
        },
    ],
}
