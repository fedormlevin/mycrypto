#!/usr/bin/env python

import os
from packages.websocket_handler2 import WebSocketClient
import logging
import hmac
import hashlib
from dotenv import load_dotenv
import time
from decimal import Decimal
import math
from packages import utils
import jwt
import pandas as pd
import json
from cryptography.hazmat.primitives import serialization
from queue import Queue
import secrets
from packages.market_data_handler import MDProcessor

pd.options.display.max_columns = None

def build_jwt(service, key_name, key_secret):
    private_key_bytes = key_secret.encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None)

    jwt_payload = {
        'sub': key_name,
        'iss': "coinbase-cloud",
        'nbf': int(time.time()),
        'exp': int(time.time()) + 60,
        'aud': [service],
    }

    jwt_token = jwt.encode(
        jwt_payload,
        private_key,
        algorithm='ES256',
        headers={'kid': key_name, 'nonce': secrets.token_hex()},
    )

    return jwt_token

