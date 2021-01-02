/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "consts.h"

#ifndef REDISTIMESERIES_RESULTSET_H
#define REDISTIMESERIES_RESULTSET_H

/* Incomplete structures for compiler checks but opaque access. */
typedef struct TS_ResultSet TS_ResultSet;

TS_ResultSet *createResultSet();

void freeResultSet(TS_ResultSet *r);

// int parseGrouperAndReducer(RedisModuleCtx *ctx,
//                               RedisModuleString **argv,
//                               int argc);

#endif //REDISTIMESERIES_RESULTSET_H
