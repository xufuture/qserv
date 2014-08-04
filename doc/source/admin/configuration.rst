#####################
Software architecture
#####################

***********************
Configuration procedure
***********************

Goals
=====

Qserv installation procedure consists of next steps :

- download, build and install using eups,
- configuration.

The goal is to have a modular procedure in order to ease evolution and maintenance.
That's why Qserv configuration procedure must be as independant as possible of other steps, and of eups.

Approach
========

eups install directory must only contains immutable data like binaries.
That's why Qserv configuration tool create a separate directory which contains :

- configuration files and data,
- execution informations (pid files, log files).
- business data (i.e. sky data),

Thereafter, this directory will be called QSERV_RUN_DIR.
This will allow to switch Qserv version (using eups for example), without having to re-configure Qserv from scratch.
The configuration tool is written in pure-python and offer many options in order to be very flexible. Neverthelles it doesn't relies on LSST standards as it seems nothing is yet provided for configuration.
The default procedure aims to be straightforward, so that new users can't quickly get a standard mono-node Qserv configuration. 

Features
========

- create services configuration files in QSERV_RUN_DIR/etc/qserv.conf using a meta-configuration file in QSERV_RUN_DIR/qserv.conf 
- runs mysql, scisql and xrootd configuration scripts
- create client configuration
- default procedure doesn't remove QSERV_RUN_DIR but only update it
- a -all option allow to create QSERV_RUN_DIR from scratch (

Tickets completed
=================

- https://jira.lsstcorp.org/browse/DM-622
- https://jira.lsstcorp.org/browse/DM-930

Prospects
=========

Migration
=========

see https://jira.lsstcorp.org/browse/DM-895

- check for configuration compatibility between Qserv versions in order to switch safely Qserv versions for a given QSERV_RUN_DIR.
- create a migration procedure

Output
======

see https://jira.lsstcorp.org/browse/DM-954

- produce a clearer output
