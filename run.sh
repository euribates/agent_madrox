#!/usr/bin/env bash

cd $HOME/web/agent_madrox
source $HOME/.virtualenvs/madrox/bin/activate
python madrox.py migrate all --num-days 21

