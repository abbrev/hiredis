#include <stdarg.h>

#include "hiredis.h"

typedef struct clusterContext {
	redisContext *context;
} clusterContext;

clusterContext *clusterConnect(const char *host, int port);

redisReply *clustervCommand(clusterContext *cluster, const char *fmt, va_list ap);
redisReply *clusterCommand(clusterContext *cluster, const char *fmt, ...);

void clusterFree(clusterContext *cluster);

