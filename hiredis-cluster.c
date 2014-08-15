#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

#include "hiredis-cluster.h"

clusterContext *clusterConnect(const char *host, int port)
{
	clusterContext *cluster = NULL;
	
	cluster = malloc(sizeof *cluster);
	if (!cluster) {
		goto out;
	}

	cluster->context = redisConnect(host, port);

out:
	return cluster;
}

static bool handleRedirect(clusterContext *cluster, redisReply *reply)
{
	if (reply->type == REDIS_REPLY_ERROR) {
		char dest[256];
		char *host = NULL, *portstr = NULL;
		int n = 0;
		bool ask = false;
		if (strncmp(reply->str, "MOVED ", 6) == 0) {
			// connect to new server and try command again
			//fprintf(stderr, "%s: reply: \"%s\"\n", __func__, reply->str);
			n = sscanf(reply->str, "MOVED %*d %s", dest);
		} else if (strncmp(reply->str, "ASK ", 4) == 0) {
			// TODO test this!
			fprintf(stderr, "TEST THIS! got ask reply: \"%s\"\n", reply->str);
			n = sscanf(reply->str, "ASK %*d %s", dest);
			ask = true;
		}
		if (n == 1) {
			host = strtok(dest, ":");
			portstr = strtok(NULL, ":");
			int port = atoi(portstr);

			freeReplyObject(reply);
			reply = NULL;
			redisFree(cluster->context);

			//fprintf(stderr, "%s: connecting to %s:%d\n", __func__, host, port);
			cluster->context = redisConnect(host, port);
			if (cluster->context->err) {
				//fprintf(stderr, "%s: error connecting to %s:%d (%s)\n", __func__, host, port, cluster->context->errstr);
				return false;
			}
			if (ask) {
				reply = redisCommand(cluster->context, "ASKING");
			}
			return true;
		}
	}
	return false;
}

redisReply *clustervCommand(clusterContext *cluster, const char *fmt, va_list ap)
{
	redisReply *reply = NULL;
	//fprintf(stderr, "%s: executing command \"%s\"\n", __func__, fmt);

	do {
		if (cluster->context->err) {
			freeReplyObject(reply);
			reply = NULL;
			break;
		}

		va_list aq;
		va_copy(aq, ap);

		reply = redisvCommand(cluster->context, fmt, aq);

		va_end(ap);
	} while (handleRedirect(cluster, reply));
	return reply;
}

redisReply *clusterCommandArgv(clusterContext *cluster, int argc, const char **argv, const size_t *argvlen)
{
	redisReply *reply = NULL;
	//fprintf(stderr, "%s: executing command \"%s\"\n", __func__, fmt);

	do {
		if (cluster->context->err) {
			freeReplyObject(reply);
			reply = NULL;
			break;
		}

		reply = redisCommandArgv(cluster->context, argc, argv, argvlen);
	} while (handleRedirect(cluster, reply));
	return reply;
}

redisReply *clusterCommand(clusterContext *cluster, const char *fmt, ...)
{
	redisReply *reply = NULL;

	va_list ap;
	va_start(ap, fmt);

	reply = clustervCommand(cluster, fmt, ap);

	va_end(ap);

	return reply;
}

void clusterFree(clusterContext *cluster)
{
	if (!cluster) return;

	redisFree(cluster->context);
	free(cluster);
}
