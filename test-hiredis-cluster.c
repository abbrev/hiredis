#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

#include "hiredis-cluster.h"

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

