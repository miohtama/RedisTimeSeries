/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "resultset.h"

#include "rax.h"
#include "string.h"
#include "tsdb.h"

#include "rmutil/alloc.h"

typedef struct TS_ResultSet
{
    rax *groups;
    char *groupbylabel;
    uint64_t total_groups;
} TS_ResultSet;

TS_ResultSet *createResultSet() {
    TS_ResultSet *r = malloc(sizeof(TS_ResultSet));
    r->groups = raxNew();
    r->groupbylabel = NULL;
    r->total_groups = 0;
    return r;
}

int groupbyLabel(TS_ResultSet *r, char *label) {
    r->groupbylabel = strdup(label);
    return true;
}

int applyReducerToResultSet(TS_ResultSet *r, char *reducer) {
    // no need to apply reducer to single series
    // if (r->groupbylabel == NULL) {
    //     return true;

    // Labels:
    // <label>=<groupbyvalue>
    // __reducer__=<reducer>
    // __source__=key1,key2,key3
    CreateCtx cCtx = { 0 };
    cCtx.labels = malloc(sizeof(Label) * 3);
    cCtx.labels[0].key = RedisModule_CreateStringPrintf(NULL, "%s", r->groupbylabel);
    cCtx.labels[0].value = RedisModule_CreateStringPrintf(NULL, "%s", "aaaa");
    cCtx.labels[1].key = RedisModule_CreateStringPrintf(NULL, "__reducer__");
    cCtx.labels[1].value = RedisModule_CreateStringPrintf(NULL, "max");
    cCtx.labels[2].key = RedisModule_CreateStringPrintf(NULL, "__source__");
    cCtx.labels[2].value = RedisModule_CreateStringPrintf(NULL, "");
    cCtx.labelsCount = 3;

    raxIterator ri;
    raxStart(&ri, r->groups);
    raxSeek(&ri, "^", NULL, 0);
    char *source_keys = strdup("");
    Series *new =
        NewSeries(RedisModule_CreateStringPrintf(NULL, "%s=%s", r->groupbylabel, "aaaa"), &cCtx);
    double value = 0.0;
    while (raxNext(&ri)) {
        if (r->groupbylabel != NULL) {
            TS_ResultSet *innerResultSet = (TS_ResultSet *)ri.data;
            applyReducerToResultSet(innerResultSet, reducer);
            // we should now have a serie per label
            // TODO: re-seek and reduce
            // innerResultSet->groupbylabel = NULL;
        } else {
            raxInsert(r->groups, (unsigned char *)ri.key, ri.key_len, new, NULL);

            // Series *newRangeSerie;
            // Series *originalCurrentSerie;
            // originalCurrentSerie = (Series *)ri.data;
            // ApplySerieRangeIntoNewSerie(&newRangeSerie,
            //                             originalCurrentSerie,
            //                             start_ts,
            //                             end_ts,
            //                             aggObject,
            //                             time_delta,
            //                             maxResults,
            //                             rev);
            // raxInsert(r->groups, (unsigned char *)ri.key, ri.key_len, newRangeSerie, NULL);
        }
    }
    raxStop(&ri);
    // cCtx.labels[2].value = RedisModule_CreateStringPrintf(NULL, "%s");
    return true;
}

// let's first make the max work
int applyRangeToResultSet(TS_ResultSet *r,
                          api_timestamp_t start_ts,
                          api_timestamp_t end_ts,
                          AggregationClass *aggObject,
                          int64_t time_delta,
                          long long maxResults,
                          bool rev) {
    raxIterator ri;
    raxStart(&ri, r->groups);
    raxSeek(&ri, "^", NULL, 0);

    double value = 0.0;
    while (raxNext(&ri)) {
        if (r->groupbylabel != NULL) {
            TS_ResultSet *innerResultSet = (TS_ResultSet *)ri.data;
            applyRangeToResultSet(
                innerResultSet, start_ts, end_ts, aggObject, time_delta, maxResults, rev);
        } else {
            Series *newRangeSerie;
            Series *originalCurrentSerie;
            originalCurrentSerie = (Series *)ri.data;
            ApplySerieRangeIntoNewSerie(&newRangeSerie,
                                        originalCurrentSerie,
                                        start_ts,
                                        end_ts,
                                        aggObject,
                                        time_delta,
                                        maxResults,
                                        rev);
            raxInsert(r->groups, (unsigned char *)ri.key, ri.key_len, newRangeSerie, NULL);
        }
    }
    raxStop(&ri);
    return true;
}

int addSerieToResultSet(TS_ResultSet *r, Series *serie, const char *name) {
    const size_t namelen = strlen(name);
    int result = false;
    // If we're grouping by label then the rax associated values are TS_ResultSet
    // If we're grouping by name ( groupbylabel == NULL ) then the rax associated values are Series
    if (r->groupbylabel != NULL) {
        char *labelValue = SeriesGetCStringLabelValue(serie, r->groupbylabel);
        const size_t labelLen = strlen(labelValue);
        printf("labelValue: %s\n", labelValue);
        TS_ResultSet *labelGroup = raxFind(r->groups, (unsigned char *)labelValue, labelLen);
        if (labelGroup == raxNotFound) {
            labelGroup = createResultSet();
            r->total_groups++;
            raxInsert(r->groups, (unsigned char *)labelValue, labelLen, labelGroup, NULL);
            printf("creating new TS_ResultSet for label: %s\n", labelValue);
        }
        addSerieToResultSet(labelGroup, serie, name);
        printf("Total series for label: %s : %d\n", labelValue, labelGroup->total_groups);
    } else {
        // If a serie with that name already exists we return
        if (raxFind(r->groups, (unsigned char *)name, namelen) != raxNotFound) {
            return false;
        }
        raxInsert(r->groups, (unsigned char *)name, namelen, serie, NULL);
        r->total_groups++;
    }
    return result;
}

void replyResultSet(RedisModuleCtx *ctx,
                    TS_ResultSet *r,
                    bool withlabels,
                    api_timestamp_t start_ts,
                    api_timestamp_t end_ts,
                    AggregationClass *aggObject,
                    int64_t time_delta,
                    long long maxResults,
                    bool rev) {
    RedisModule_ReplyWithArray(ctx, r->total_groups);
    raxIterator ri;
    raxStart(&ri, r->groups);
    raxSeek(&ri, "^", NULL, 0);
    Series *s;
    while (raxNext(&ri)) {
        if (r->groupbylabel != NULL) {
            TS_ResultSet *innerResultSet = (TS_ResultSet *)ri.data;
            replyResultSet(ctx,
                           innerResultSet,
                           withlabels,
                           start_ts,
                           end_ts,
                           aggObject,
                           time_delta,
                           maxResults,
                           rev);
        } else {
            s = (Series *)ri.data;
            RedisModule_ReplyWithArray(ctx, 3);
            RedisModule_ReplyWithString(ctx, s->keyName);
            if (withlabels) {
                ReplyWithSeriesLabels(ctx, s);
            } else {
                RedisModule_ReplyWithArray(ctx, 0);
            }
            ReplySeriesRange(ctx, s, start_ts, end_ts, aggObject, time_delta, maxResults, rev);
        }
    }
    raxStop(&ri);
}

void freeResultSet(TS_ResultSet *r) {
    if (r == NULL)
        return;
    if (r->groups) {
        // if
        if (r->groupbylabel) {
            raxFreeWithCallback(r->groups, freeResultSet);
        } else {
            raxFree(r->groups);
        }
    }
    if (r->groupbylabel)
        free(r->groupbylabel);
    free(r);
}
