package art

/*
Bulk loading: When an index is created for an existing
relation, the following recursive algorithm can be used to speed
up index construction: Using the first byte of each key the
key/value pairs are radix partitioned into 256 partitions and an
inner node of the appropriate type is created. Before returning
that inner node, its children are created by recursively applying
the bulk loading procedure for each partition using the next
byte of each key.
*/
