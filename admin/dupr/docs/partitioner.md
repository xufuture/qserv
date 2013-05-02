Requirements
------------

The Qserv partitioner must be capable of processing:
 - CSV-format files
 - files that are very large
 - large quantities of files
 - tables containing both positions and positional match pairs

It must be possible to reorder and drop input columns. The output format shall
be CSV suitable for loading into the database instances running on Qserv worker
nodes.

The following are for future consideration:
 - It should be possible to process FITS table files.
 - It should be possible to lookup the sky-positions of entities in a
   match table record by key. Currently, the positions must be present in
   the records fed to the partitioner (but can be dropped from the output).
 - Producing partitioned output tables in native database format (e.g.
   directly producing `.MYD` files for the MySQL MyISAM storage engine)
   may significantly speed up the loading process.

Design
------

The partitioner is expressed in the map-reduce paradigm. The map function
operates on one input record at a time. It computes the partition(s)
the input record must be assigned to and emits corresponding output
records. Output records are grouped by chunk and passed on to reducers,
which collect statistics on how many records are in each sub-chunk and
write output records to disk. This is all implemented in C++, using a small
in-memory map-reduce processing framework tailored to the task. The framework
is multi-threaded - file input, processing, and output are all block oriented
and parallel.

As a result of using this model, the code that deals with parallelization is
separated from the partitioning logic, making the implementation easier to
follow. Another benefit is that porting to the Hadoop framework is at
least conceptually straightforward.
