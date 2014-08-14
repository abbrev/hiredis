#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

#include "hiredis.h"

// TODO implement a thin wrapper around the synchronous hiredis API to support
// clustering. When we receive an error reply "MOVED ...", automatically
// connect to the other server (either maintain a map of server:redisContext or
// close and reuse a singleton redisContext).

typedef struct clusterContext {
	redisContext *context;
} clusterContext;

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

static void indent(int level)
{
	for (int i = 0; i < level; ++i) {
		fprintf(stderr, " ");
	}
}

static void displayReplyRecursive(const redisReply *reply, int level)
{
	switch (reply->type) {
		case REDIS_REPLY_STATUS:
			//The command replied with a status reply. The status
			//string can be accessed using reply->str. The length
			//of this string can be accessed using reply->len.
			fprintf(stderr, "status: %s\n", reply->str);
			break;

		case REDIS_REPLY_ERROR:
			//The command replied with an error. The error string
			//can be accessed identical to REDIS_REPLY_STATUS.
			fprintf(stderr, "error: %s\n", reply->str);
			break;

		case REDIS_REPLY_INTEGER:
			//The command replied with an integer. The integer
			//value can be accessed using the reply->integer field
			//of type long long.
			fprintf(stderr, "%lld\n", reply->integer);
			break;

		case REDIS_REPLY_NIL:
			//The command replied with a nil object. There is no
			//data to access.
			fprintf(stderr, "(nil)\n");
			break;

		case REDIS_REPLY_STRING:
			//A bulk (string) reply. The value of the reply can be
			//accessed using reply->str. The length of this string
			//can be accessed using reply->len.
			fprintf(stderr, "\"%s\"\n", reply->str);
			break;

		case REDIS_REPLY_ARRAY:
			//A multi bulk reply. The number of elements in the
			//multi bulk reply is stored in reply->elements. Every
			//element in the multi bulk reply is a redisReply
			//object as well and can be accessed via
			//reply->element[..index..]. Redis may reply with
			//nested arrays but this is fully supported.
			//fprintf(stderr, "array reply (%zu elements):\n", reply->elements);
			for (size_t i = 0; i < reply->elements; ++i) {
				indent(i == 0 ? 0 : level);
				fprintf(stderr, "%2zu) ", i);
				displayReplyRecursive(reply->element[i], level + 4);
			}
			break;
		default:
			fprintf(stderr, "unknown reply type (%d)\n", reply->type);
	}
}

static void displayReply(const redisReply *reply)
{
	displayReplyRecursive(reply, 0);
}

redisReply *clustervCommand(clusterContext *cluster, const char *fmt, va_list ap)
{
	redisReply *reply = NULL;
	fprintf(stderr, "%s: executing command \"%s\"\n", __func__, fmt);

	for (;;) {
		if (cluster->context->err) {
			freeReplyObject(reply);
			reply = NULL;
			break;
		}

		va_list aq;
		va_copy(aq, ap);

		reply = redisvCommand(cluster->context, fmt, aq);

		va_end(ap);

		//displayReply(reply);
		if (reply->type == REDIS_REPLY_ERROR) {
			displayReply(reply);
			char dest[256];
			char *host = NULL, *portstr = NULL;
			int n = 0;
			bool ask = false;
			if (strncmp(reply->str, "MOVED ", 6) == 0) {
				// connect to new server and try command again
				//fprintf(stderr, "%s: reply: \"%s\"\n", __func__, reply->str);
				n = sscanf(reply->str, "MOVED %*d %s", dest);
			} else if (strncmp(reply->str, "ASK ", 4) == 0) {
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

				fprintf(stderr, "%s: connecting to %s:%d\n", __func__, host, port);
				cluster->context = redisConnect(host, port);
				if (cluster->context->err) {
					//fprintf(stderr, "%s: error connecting to %s:%d (%s)\n", __func__, host, port, cluster->context->errstr);
					break;
				}
				if (ask) {
					reply = redisCommand(cluster->context, "ASKING");
					displayReply(reply);
				}
				//usleep(100000L); // TODO remove this
				continue;
			}
		}
		break;
	}
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

static void getKey(clusterContext *cluster, const char *key)
{
	fprintf(stderr, "getting key \"%s\":\n", key);
	redisReply *reply = clusterCommand(cluster, "GET %s", key);
	if (!reply) {
		fprintf(stderr, "error executing command: %s\n", cluster->context->errstr);
		exit(1);
	}
	//fprintf(stderr, "%s (%d)\n", __FILE__, __LINE__);

	displayReply(reply);
	freeReplyObject(reply);
}

int main()
{
	clusterContext *cluster = NULL;
	redisReply *reply = NULL;

	cluster = clusterConnect("127.0.0.1", 7000);
	if (!cluster || cluster->context->err) {
		fprintf(stderr, "error connecting: %s\n", cluster ? cluster->context->errstr : "unknown");
		exit(1);
	}
	fprintf(stderr, "%s (%d)\n", __FILE__, __LINE__);

	getKey(cluster, "foo");
	getKey(cluster, "bar");
	getKey(cluster, "quux");
	getKey(cluster, "{1}a");
	getKey(cluster, "{1}b");
	getKey(cluster, "next-transaction");

	reply = clusterCommand(cluster, "WATCH {1}a {1}b {1}b");
	if (!reply) {
		fprintf(stderr, "error executing command: %s\n", cluster->context->errstr);
		exit(1);
	}
	reply = clusterCommand(cluster, "MULTI");
	if (!reply) {
		fprintf(stderr, "error executing command: %s\n", cluster->context->errstr);
		exit(1);
	}
	getKey(cluster, "{1}a");
	getKey(cluster, "{1}b");
	reply = clusterCommand(cluster, "MGET %s %s %s %s", "{1}a", "{1}b", "{1}c", "{1}foo");
	if (!reply) {
		fprintf(stderr, "error executing command: %s\n", cluster->context->errstr);
		exit(1);
	}
	displayReply(reply);
	reply = clusterCommand(cluster, "strlen %s", "{1}a");
	if (!reply) {
		fprintf(stderr, "error executing command: %s\n", cluster->context->errstr);
		exit(1);
	}
	displayReply(reply);
	getKey(cluster, "{1}c");
	reply = clusterCommand(cluster, "EXEC");
	if (!reply) {
		fprintf(stderr, "error executing command: %s\n", cluster->context->errstr);
		exit(1);
	}
	displayReply(reply);

	reply = clusterCommand(cluster, "UNWATCH");
	if (!reply) {
		fprintf(stderr, "error executing command: %s\n", cluster->context->errstr);
		exit(1);
	}
	//fprintf(stderr, "%s (%d)\n", __FILE__, __LINE__);

	displayReply(reply);
	freeReplyObject(reply);

	return 0;
}

