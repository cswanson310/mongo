# Query System Internals

*Disclaimer*: This is a work in progress. It is not complete and we will
do our best to do so in a timely manner.

## Overview

The query system generally is responsible for interpreting the user's
request, finding an optimal way to satisfy it, and to actually bring the
results. It is primarily exposed through the commands find and
aggregate, but also used in associated read commands like count,
distinct, and mapReduce.

Here we will divide it into the following phases and topics:

 * Command Parsing & Validation
 * MQL Parsing & Validation
 * Query Optimization
 ** Index Tagging
 ** Plan Enumeration
 ** Plan Selection
 ** Plan Compilation
 ** Plan Caching
 * Query Execution

Here we focus on the process for a single node or replica set where all
the data is expected to be found locally. We plan to add documentation
for the sharded case in the src/mongo/s/query/ directory later.

## Command Parsing & Validation

Here we will focus on the following commands, generally maintained by
the query team and with the majority of our focus on the first two.
* find
* aggregate
* count
* distinct
* mapReduce

The code path for each of these starts in a Command, named something
like MapReduceCommand or FindCmd.

The first round of parsing is to piece apart the command into it's
components. Notably, we don't yet try to understand the meaning of some
of the more complex arguments which are more typically considered the
"MongoDB Query Language" or MQL. For example, these are find's 'filter',
'projection', and 'sort' arguments, or the individual stages in the
'pipeline' argument to aggregate.

In contrast, here we just take the incoming BSON object and piece it
apart into a C++ struct with separate storage for each argument, keeping
the MQL elements as mostly unexamined BSON for now. For example, going
from one object with {filter: {}, skip: 4, limit: 5} into a C++ object
which stores those things as member variables. Here we prefer using an
Interface Definition Language (IDL) tool to generate the parser and
actually generate the C++ class itself.

## The Interface Definition Language

You can find some files ending with '.idl' as examples, a snippet may
look like this:

```
commands:
    count:
        description: "Parser for the 'count' command."
        command_name: count
        cpp_name: CountCommand
        strict: true
        namespace: concatenate_with_db_or_uuid
        fields:
            query:
                description: "A query that selects which documents to count in the collection or
                view."
                type: object
                default: BSONObj()
            limit:
                description: "The maximum number of matching documents to count."
                type: countLimit
                optional: true
```

This file (specified in a YAML format) is used to generate C++ code. Our
build system will run a python tool to parse this YAML and spit out C++
code which is then compiled and linked. This code is left in a file
ending with '\_gen.h' or '\_gen.cpp', for example
'count\_command\_gen.cpp'. You'll notice that things like whether it is
optional, the type of the field, and any defaults are included here, so
we don't have to write any code to handle that.

The generated file will have methods to get and set all the members, and
will return a boost::optional for optional fields. In the example above,
it will generate a CountCommand::getQuery() method, among others.

## Other actions performed during this stage

As stated before, the MQL elements are unparsed - the query here is
still an "object", stored in BSON without any scrutiny at this point.

This is how we begin to transition into the next phase where we piece
apart the MQL. Before we do that, there are a number of important things
that happen on these structures.

### Authorization checking

In many but not all cases, we will only need to parse this much to check
whether the user is allowed to perform this request. We usually only need the
type of command and the namespace to do this. In the case of mapReduce,
we also take into account whether the command will perform writes based
on the output format. A more notable exception is the aggregate command,
where different stages can read different types of data which require
special permissions. For example a pipeline with a $lookup or a
$currentOp may require additional privileges beyond just the namespace
given to the command.

