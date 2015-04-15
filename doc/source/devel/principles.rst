.. _principles:


######
Guiding principles
######

******
Make entities disposable where possible
******
The system should, in general, survive faults where entities crash,
hang, or corrupt. The system should continue operation if the czar, proxy,
xrootd server, xrootd redirector, zookeeper, or mysqld
disappear. When an element is lost, the system should only lose the
resources under control of that element and no other element. Outages
and data loss can still occur if a large fraction 
of resources are lost.

Example: If the czar instance hangs, only queries managed by that czar
are lost. Minimize loss of long-running queries on the failed czar by
recording state information of long-running queries on a durable
component.

*****
Component responsibility
*****

 * Czar: Manages user queries. Dispatches to workers and aggregates
   results. Query analysis. Recover from worker failures.

 * Worker: Perform queries on chunks. Manage subchunks. Keep chunks
   consistent (detect corruption and re-replicate). Support addition
   of new data.

 * CSS (Zookeeper): Maintain slowly-changing system metadata. Updates
   should not normally exceed what a human can perform with no automated
   assistance (e.g., update frequency < 1/sec). The low update
   frequency can be considered for caching its state.

 * mysql-proxy: Provide a mysql-protocol interface for
   users. Interface with czar. Plumbing for incoming queries and
   returning final results. In general, shouldn't do any "real work"
   other than selecting the right RPC functions to call on other qserv
   elements (e.g., czar).

 * xrootd/cmsd manager, supervisors: Provide cached redirection service to
   locate xrootd servers with resources. Failover gracefully.

 * xrootd/cmsd server: xrootd-protocol interface to worker for query
   execution.



**************
Minimize open-ports
**************

Internal Qserv communication should be done without TCP numbered
        sockets (host:port). Instead, use UNIX socket files--they can
        only be accessed by process on the same machine.

Rationale: External-facing interfaces can be hit by port-scanners and
are potential security holes. Each external interface must be hardened
against malicious and fuzz/garbage connections.

Example: worker mysqld and czar mysqld are internal, so they should not listen on a port.

Exception: czar listens on a port to accept external XML-RPC
        connections (from the proxy or advanced users). It is external-facing.

Exception: mysql proxy listens on a port because it is the high-level
  external interface to Qserv.

Exception: zookeeper instances are internal, but they listen on ports
  because czar instances (and loader, etc) connect to them from
  different machines.

Exception: for debugging


*****
Hide use of mysqld
*****
Use the abstraction around mysql connection. Don't bypass it, unless
using it would require re-wrapping the needed features at a low level
where the abstraction is not helping. 

In general, do not create tools that access the mysqld directly except
where strictly necessary.

A worker's mysqld should be hidden by its accompanying xrootd+qserv-worker.

Rationale: We prohibit direct access in order to allow use of
        another dbms package (e.g., mariadb, postgres, monetdb,
        etc.). 



