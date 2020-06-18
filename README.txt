Rothko LSM backed by Tardigrade
--------------------------------

What is an LSM
--------------

A Log-Structured Merge (LSM) tree is a collection of sorted
key/value pairs. It is organized as a sequence of levels.
A level is a collection of files containing the key/value pairs.
All empty new files are created at level 0, and files can be merged
into a larger file. When a collection of files with maximum level N is
merged into a new file, it goes in to level N+1. In this way, files
at higher levels are typically larger and contain older data: a key
at level N is more recently written then one at a larger level M.

Merging is efficient because each file contains the keys in sorted
order, so that only linear reads and writes are necessary.
Additionaly, reads can use binary search inside of a file to quickly
locate a key.

Each file in a level has a generation number associated with it
so that you can tell if a file is newer within the same level.

For example, if you had files named

    L00-000100
    L00-000101
    L00-000102
    L00-000103
    L01-000090
    L02-000000

then you have 4 files at level 0 with generations 100, 101, 102 and
103, 1 file at level 1 with generation 90, and 1 file at level 2 with
generation 0. When merging files, the output file generation is the
minimum of the input generations, keeping it older. Reads would proceed
by consulting files in descending generation order (newest files first).


Atomic Directory Updates
------------------------

Compaction is the name of the process that merges files, creating a new
file and removing the merged ones. We want to do these additions and
removals atomically but creating and removing files are individually
atomic units with no obvious way to combine them into one atomic unit.
This design solves this by using symlinks to point at entire directories
of files, and updating the symlink atomically when compaction completes.

Here is an example directory structure of an LSM tree with ongoing writes
as well as an ongoing compaction. It contains a current symlink that points
at the active gen folder. The active folder never changes contents. There
is a tmp folder used to stage files that are being created from compaction.
The active gen folder always contains a single write ahead log (WAL) file.
The WAL is where new writes occur and has the property that it can be
resumed and replayed in the event of a crash. When full, it becomes a L0
file and a new WAL file is created.

    ├── current -> gen000004
    ├── gen000004
    │   ├── L0-000100
    │   ├── L0-000101
    │   ├── L0-000102
    │   ├── L1-000090
    │   ├── L2-000000
    │   └── WAL-000103
    └── tmp
        └── L3-000000

We will consider the transitions that happen as the WAL file fills to
completion and the compaction finishes. Keep in mind that during any
step in this process, if the database were to crash, it would, on startup

    1. Delete any gen folders that are not pointed at by current
    2. Delete any file in the tmp folder

In this case, that would mean that if the database crashed right now
it would not have to delete any gen folders and it would delete the
L3-000000 file as it is in tmp, aborting the compaction that was
occurring before the crash.

Lets assume that the WAL file fills up. We would transition it to an
L0 file by writing out a gen000005 folder. The files in gen000005 are
hard links to the files in gen000004 with the exception of WAL-000104
which is a freshly allocated file to accept writes as the WAL.
L0-000103 is a hard link to the WAL-000103 file in gen0000004.

This makes the directory tree become:

    ├── current -> gen000004
    ├── gen000004
    │   ├── L0-000100
    │   ├── L0-000101
    │   ├── L0-000102
    │   ├── L1-000090
    │   ├── L2-000000
    │   └── WAL-000103
    ├── gen000005
    │   ├── L0-000100
    │   ├── L0-000101
    │   ├── L0-000102
    │   ├── L0-000103
    │   ├── L1-000090
    │   ├── L2-000000
    │   └── WAL-000104
    └── tmp
        └── L3-000000

After gen000005 is synced to disk, the current symlink is changed
to point to gen000005 and gen000004 can be deleted leaving:

    ├── current -> gen000005
    ├── gen000005
    │   ├── L0-000100
    │   ├── L0-000101
    │   ├── L0-000102
    │   ├── L0-000103
    │   ├── L1-000090
    │   ├── L2-000000
    │   └── WAL-000104
    └── tmp
        └── L3-000000

Then, assuming every file besides L0-000103 was part of the compaction and
that compaction now finishes, a gen000006 folder would be created and the
directory tree would be:

    ├── current -> gen000005
    ├── gen000005
    │   ├── L0-000100
    │   ├── L0-000101
    │   ├── L0-000102
    │   ├── L0-000103
    │   ├── L1-000090
    │   ├── L2-000000
    │   └── WAL-000104
    ├── gen000006
    │   ├── L0-000103
    │   ├── L3-000000
    │   └── WAL-000104
    └── tmp
        └── L3-000000

The L3-000000 file in gen000006 would be a hard link to the file in the
tmp directory. The symlink would then be updated, gen000005 would be
deleted, and the file under tmp would be deleted, leaving the final state

    ├── current -> gen000006
    ├── gen000006
    │   ├── L0-000103
    │   ├── L3-000000
    │   └── WAL-000104
    └── tmp

*********************  WIPLOLOL  **********************
Write Ahead Log
---------------

In this design, the write ahead log is just an incomplete L0 file.
This is to avoid writing all of the key/value pairs twice while
retaining all of the benefits. So the L0 file must be designed to
perform as a WAL.

Keys
----

A key is a 128 bit value composed of a tag key hash and a metric
key hash.

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------------------------------------------------------+
|                                                               |
+                          Tag Key Hash                         +
|                                                               |
+---------------------------------------------------------------+
|                                                               |
+                          Metric Hash                          +
|                                                               |
+---------------------------------------------------------------+


*********************  WIPLOLOL  **********************


Level 0 Data Layout
-------------------

An L0 file is an unsorted 2MiB sequence of key/value entries followed
by an index describing the sorted order of the keys. The keys are
written unsorted because they are appended as the writes come in.
This means the file can be appended incrementally, allowing it to
perform as a WAL.

(TODO: better background information on metrics, values, and
       compaction merging values into larger buckets)

A key is made up of a tag key hash, a metric hash, and a timestamp.
The tag key hash is a hash over just the tag key portion of the
tags making up the metric. The metric hash is a hash over all of
the tags making up the metric. The timestamp describes the number
of minutes since unix epoch time that the entry starts at. The
duration spanned by the entry depends on what level it is contained
in: 2^level minutes. This is so that each subsequent level can
merge values and reduce space usage for historical data.

Each entry has the following bit pattern header followed immediately
by the variable length value.

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------------------------------------------------------+
|                             Length                            |
+---------------------------------------------------------------+
|                                                               |
+                                                               +
|                                                               |
+                              Key                              +
|                                                               |
+                                                               +
|                                                               |
+---------------------------------------------------------------+
|                           Timestamp                           |
+---------------------------------------------------------------+

The length describes total length of the entry including the header.
A length of zero is invalid and indicates that there is no entry
present. Every entry must be aligned to a 32 byte boundary.

The first 32 bytes of the file are reserved.

The trailer is a sequence of 2 byte indicies describing which
entry comes next in sorted order. It begins at the 2MiB byte
offset and continues until the end of the file. The maximum
size of an index is 2^17 bytes, or 128KiB.

level0 files should be written out as values are added to them with
some buffering: say 4KiB. The initial file should be pre-allocated
up to 2MiB+128KiB so that the blocks can be laid out contiguously
on disk and that there is no out of disk space condition to worry
about. Because the length describes the whole entry size, a length
of zero means that the entry does not exist and so, if it is not the
last entry, the file is incomplete. Because the first 32 bytes are
reserved, an index entry pointing to zero means that the index is
incomplete. It should be possible to open a level0 file pointing
at an incomplete file and resume it.

Level N
-------

Files at level N > 0 are written out as a sequence of blocks. Each
block is a 4 KiB key index structure followed by 124 KiB of
value data, whichever fills first. This means there is a key block
every 128 KiB.

The key index structure combines common tag key hash and tag value
hashes so that each entry begins like the following.

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------------------------------------------------------+
|                             Length                            |
+---------------------------------------------------------------+
|                                                               |
+                          Tag Key Hash                         +
|                                                               |
+---------------------------------------------------------------+
|                                                               |
+                          Metric Hash                          +
|                                                               |
+---------------------------------------------------------------+
|                          Value Offset                         |
+---------------------------------------------------------------+

The value offset is the absolute offset in to the 124 KiB block of
values where the first value starts. It is followed by pairs of
varint encoded timestamp and length values. Additionaly, the
timestamps are delta encoded.

The length is still the overall length including all of the timestamp
and length pairs. It is possible that there are more timestamp/length
pairs than can fit in the remaining space of the block. If so
the next block will contain another entry for the same key.

This scheme was chosen because it allows efficient encoding of all of
the keys by only storing them once (except in rare cases) and delta
encoding the timestamps. It still allows efficient binary search
by splitting the file into 128 KiB blocks and searching there. Since
reads from disk happen in large blocks anyway, we pack as much data
about the index as possible. Because we don't have a single index
at the start or end of the file, we don't have to hold all of it in
memory at once, so we can do compation and seeks in O(1) space.

A final 128 KiB block is appended and reserved. It is expected to be
used to store bloom filters, version information or other metadata.
