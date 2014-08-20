#include "hiredis-cluster.h"
#include "hiredis-api.h"

// watch/unwatch keys:
redisReply *clusterWatch(clusterContext *c, const char *key)
{
	redisReply *reply = clusterCommand(c, "WATCH %s", key);
	return reply;
}

redisReply *clusterUnwatch(clusterContext *c)
{
	redisReply *reply = clusterCommand(c, "UNWATCH");
	return reply;
}

// transaction:
redisReply *clusterMulti(clusterContext *c)
{
	redisReply *reply = clusterCommand(c, "MULTI");
	return reply;
}

redisReply *clusterExec(clusterContext *c)
{
	redisReply *reply = clusterCommand(c, "EXEC");
	return reply;
}

// get a value:
redisReply *clusterGet(clusterContext *c, const char *key)
{
	redisReply *reply = clusterCommand(c, "GET %s", key);
	return reply;
}

enum setX { SET_ALWAYS, SET_IF_EXISTS, SET_IF_NOT_EXISTS, };
static redisReply *set(clusterContext *c, const char *key, const char *value, long long ttl, enum setX setX)
{
	char ttlstr[30];

	int argc = 0;
	const char *argv[10];

	argv[argc++] = "SET";
	argv[argc++] = key;
	argv[argc++] = value;

	if (ttl != 0) {
		sprintf(ttlstr, "%lld", ttl);
		argv[argc++] = "PX";
		argv[argc++] = ttlstr;
	}

	if (setX == SET_IF_EXISTS) {
		argv[argc++] = "XX";
	} else if (setX == SET_IF_NOT_EXISTS) {
		argv[argc++] = "NX";
	}

	redisReply *reply = NULL;
	redisReply *clusterCommandArgv(clusterContext *cluster, int argc, const char **argv, const size_t *argvlen);

	reply = clusterCommandArgv(c, argc, argv, NULL);

	return reply;
}

// modify value:
redisReply *clusterSet(clusterContext *c, const char *key, const char *value, long long ttl)
{
	return set(c, key, value, ttl, SET_ALWAYS);
}

redisReply *clusterSetIfExists(clusterContext *c, const char *key, const char *value, long long ttl)
{
	return set(c, key, value, ttl, SET_IF_EXISTS);
}

redisReply *clusterSetIfNotExists(clusterContext *c, const char *key, const char *value, long long ttl)
{
	return set(c, key, value, ttl, SET_IF_NOT_EXISTS);
}

redisReply *clusterIncr(clusterContext *c, const char *key, long long step)
{
	redisReply *reply = clusterCommand(c, "INCR %s %lld", key, step);
	return reply;
}

redisReply *clusterDecr(clusterContext *c, const char *key, long long step)
{
	redisReply *reply = clusterCommand(c, "DECR %s %lld", key, step);
	return reply;
}


// delete a key:
redisReply *clusterDel(clusterContext *c, const char *key)
{
	redisReply *reply = clusterCommand(c, "DEL %s", key);
	return reply;
}


