"""Google Cloud function that uploads the conversion to GADS"""

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -*- coding: utf-8 -*-

import base64
import datetime
import json
import os
import re
import sys
import uuid
# import pandas as pd
# import numpy as np

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import pubsub_v1
from google.cloud import firestore

import pytz



def main(request):
  """Triggers the conversion upload.

  Args:
    request (flask.Request): HTTP request object.
  Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
  """
  del context
  
  print("Main")
  

