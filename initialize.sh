#!/usr/bin/env bash

# first time setup for vitex_stats_server

source prod_env.sh
flask manage create-tables
flask manage download-sbp
flask manage download-tokens
