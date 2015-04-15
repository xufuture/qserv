.. _future-work:

=============
 Future work
=============

There is always more to be done. Here are some major areas which
should be considered before working on that great idea you had that
will only take a week.


User privilege management
=========================

Split QueryContext into QueryContext and QueryClipboard
=======================================================


Handle nested query namespaces for queries
==========================================
This may be necessary to actually handle subqueries. This means that
subqueries inherit their parent query context/namespace, but none of
their additions should pollute their parent's context/namespace.


*******
Multi-spindle I/O management on the worker
*******

*******
Czar/worker health monitoring/managment
*******
Allow getting/setting of tuning parameters (max queries in flight, max
time for interactive queries, query scheduling parameters, etc).

*******
Updated secondary indexing
*******
InnoDb-based indexing is probably not going to scale for 50 billion
rows. Maybe tokudb will?

Support for multiple secondary indexes (per-director table)? This
could be very tricky.

*******
Template queries in dispatch protocol
*******
Currently, the czar transfers complete query text to workers. Consider
sending a template with placeholders, along with tuples to
substitute. 

qtemplate = "SELECT * FROM Object_{1}"
parameterlist =  [("12312"), ("12314")]

*******
Better ID usage
*******
user query id: GUID
user query hash: used to detect cache opportunities


*******
Pre-materialization of common aggregates
*******
Pre-compute AVG,MIN,MAX for all columns in chunk tables. Now when the
czar encounters any plain full-sky aggregate without a where
condition, it can request the pre-computed answers. This can still be
leveraged if there are spatial restrictions, for the fully-covered
chunks, but this is a bit more tricky.






REST between proxy and czar instead of XML-RPC
==============================================
This is probably a good idea, although we would need to implement the
argument and result marshalling on both sides. But, aside from the
aesthetic improvement and dropping a dependency on XML, the
benefit is minimal. 



"beside the database" processing
================================
Multiple components involved here.

Idea:
* User uploads executable to system. 
* User submits SQL query whose results are used as input to
  executable.
* Query execution proceeds normally, except:
  * At worker, executable is spawned within container.
  * within container, executable is started, with a parameter
    indicating chunk number, and query results are piped in using
    a format like the output of mysql client.
    * echo "SELECT column FROM table WHERE..." | mybinary --chunk
      123 > output
  * output files are saved in user's home space as:
    results.qserv.<queryid>.chunk


   



Wishlists
=========

See: https://dev.lsstcorp.org/trac/wiki/db/Qserv/RedesignFY2014
