/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "resultset.h"

#include "rax.h"

#include "rmutil/alloc.h"

typedef struct TS_ResultSet
{
    rax *series;
    rax *labels;
} TS_ResultSet;

TS_ResultSet *createResultSet() {
    TS_ResultSet *r = malloc(sizeof(TS_ResultSet));
    r->series = raxNew();
    r->labels = raxNew();
    return r;
}

void freeResultSet(TS_ResultSet *r) {
    if (r == NULL)
        return;
    if (r->series)
        raxFree(r->series);
    if (r->labels)
        raxFree(r->labels);
    free(r);
}
