#!/bin/bash

# Description: It creates new SQL database for FUME emission model.

# This file is part of the FUME emission model.
#
# FUME is free software: you can redistribute it and/or modify it under the terms of the GNU General
# Public License as published by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# FUME is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
# Information and source code can be obtained at www.fume-ep.org
#
# Copyright 2014-2023 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
# Copyright 2014-2023 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
# Copyright 2014-2023 Czech Hydrometeorological Institute, Prague, Czech Republic
# Copyright 2014-2017 Czech Technical University in Prague, Czech Republic


##############################
echo "This script MUST be run on PG server with user postgres or"
echo "another postgres user with privilege to create database."
echo "Press [Enter] key when ready to continue, press Ctrl+C to exit."
read anytext
echo -n "Enter database name: "; read dbname
echo -n "Enter user name or press [Enter] to use your name: "; read username
echo ""
##############################

if [ "x$dbname" == "x" ]; then
  echo "Use: ep_create_database.sh <database_name> [<user_name>]"
  exit 1
fi

dbexists=$(psql -l | cut -d"|" -f1|grep -c "$dbname")
if [ ! $dbexists == 0 ]; then
  echo "Database $dbname just exists on the server!"
  echo "Use another database name or drop the existing database first."
  exit 1
fi


nr=$(psql -qtA -d postgres -c "select count(*) from pg_catalog.pg_roles where rolname = 'emisproc'")
if [ $nr == 0 ]; then
  echo "Create \"emisproc\" group role"
  psql -d postgres -c "create role \"emisproc\" createdb nologin"
fi

if [ "x$username" == "x" ]; then
  username=$USER
fi

nr=$(psql -qtA -d postgres -c "select count(*) from pg_catalog.pg_roles where rolname = '$username'")
if [ $nr == 0 ]; then
  echo "Create \"$username\" user role"
  echo -n "Enter password for user $username: "; read userpass
  psql -d postgres -c "create role \"$username\" login in role \"emisproc\""
  psql -d postgres -c "alter user \"$username\" with password '$userpass';"
fi

echo "Create database: $dbname, with owner \"emisproc\""
psql -d postgres -c "create database \"$dbname\" owner \"emisproc\";"
psql -d $dbname -c "create extension postgis;"
psql -d $dbname -c "create extension postgis_topology;"
psql -d $dbname -c "create extension intarray;"
psql -d $dbname -c "grant all on database \"$dbname\" to \"emisproc\" with grant option;"
psql -d $dbname -c "grant all on spatial_ref_sys to \"emisproc\";"
psql -d $dbname -c "grant all on spatial_ref_sys_srid_seq to \"emisproc\";"

echo "Database $dbname has been created and postgis has been enabled in it."
echo "Connect to the database as user $username and run script ep_create_database.sql."
echo "To run it you can use command:"
echo "psql -h <hostname> -p <port> -U <username> [-W] -d $dbname -f ep_create_database.sql"
echo "Current connection info is:"
psql -d $dbname -c "\conninfo"

#psql -d $dbname -c "alter role $username with SUPERUSER CREATEDB CREATEROLE CREATEUSER INHERIT LOGIN REPLICATION BYPASSRLS;"
