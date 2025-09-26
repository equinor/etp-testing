# ARCHIVED
See https://github.com/equinor/pyetp


# Demo of using websockets in Python to communicate with an ETP server
The purpose of this demo is to give some examples on how you can use websockets to communicate with an ETP v12 server. The standard can be downloaded from here: [https://publications.opengroup.org/standards/energistics-standards/energistics-transfer-protocol/v234](https://publications.opengroup.org/standards/energistics-standards/energistics-transfer-protocol/v234). The code in `talk_to_open_etp_server.py` demonstrates many of the protocols from the standard, and hopefully gives ideas on how other protocols can be added.

## Test script locally
Set up the `open-etp-server` ([https://community.opengroup.org/osdu/platform/domain-data-mgmt-services/reservoir/open-etp-server](https://community.opengroup.org/osdu/platform/domain-data-mgmt-services/reservoir/open-etp-server)) via Docker compose with:
```
docker compose up --detach
```
Next, install Python requirements in a virtual environment with:
```
python -m venv venv
source venv/bin/activate
pip install pip --upgrade && pip install -r requirements.txt
```
Finally, run:
```
python talk_to_open_etp_server_generated.py
```
which does a roundtrip exercise with a small RESQML `Grid2dRepresentation`-object of random values.
