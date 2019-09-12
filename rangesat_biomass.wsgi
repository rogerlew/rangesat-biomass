#!/usr/bin/python3
import sys
import logging
logging.basicConfig(stream=sys.stderr)
sys.path.insert(0, "/var/www/rangesat-biomass/")

from api.app import app as application
application.secret_key = 'xbcn1wegtyajhib23bijrewuy'
