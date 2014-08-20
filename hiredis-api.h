#ifndef _HIREDIS_API_H_
#define _HIREDIS_API_H_

#include "hiredis-cluster.h"

// watch/unwatch keys:
redisReply *clusterWatch(clusterContext *c, const char *key); // reply is string "OK"
redisReply *clusterUnwatch(clusterContext *c);

// transaction:
redisReply *clusterMulti(clusterContext *c); // reply is string "OK"
redisReply *clusterExec(clusterContext *c); // reply is array of results (a reply for each queued command), or nil if execution was aborted

// get a value:
redisReply *clusterGet(clusterContext *c, const char *key); // reply is value of key, or nil if key does not exist

// modify value:
redisReply *clusterSet(clusterContext *c, const char *key, const char *value, long long ttl); // reply is string "OK" if value was set, or nil if value was not set
redisReply *clusterSetIfExists(clusterContext *c, const char *key, const char *value, long long ttl); // reply is string "OK" if value was set, or nil if value was not set
redisReply *clusterSetIfNotExists(clusterContext *c, const char *key, const char *value, long long ttl); // reply is string "OK" if value was set, or nil if value was not set
redisReply *clusterIncr(clusterContext *c, const char *key, long long step); // reply is integer value of key after incrementing it
redisReply *clusterDecr(clusterContext *c, const char *key, long long step); // reply is integer value of key after decrementing it

// delete a key:
redisReply *clusterDel(clusterContext *c, const char *key); // reply is integer number of keys deleted

#endif
