/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "resultset.h"

#include "rax.h"
#include "reply.h"
#include "string.h"
#include "tsdb.h"

#include "rmutil/alloc.h"

typedef enum
{
    GroupType_Series,
    GroupType_ResultSet,
} GroupType;

typedef struct TS_ResultSet
{
    rax *groups;
    GroupType groupsType;
    char *labelkey;
    char *labelvalue;
} TS_ResultSet;

TS_ResultSet *createResultSet() {
    TS_ResultSet *r = malloc(sizeof(TS_ResultSet));
    r->groups = raxNew();
    r->groupsType = GroupType_Series;
    r->labelkey = NULL;
    r->labelvalue = NULL;
    return r;
}

int setLabelValue(TS_ResultSet *r, const char *label) {
    r->labelvalue = strdup(label);
    return true;
}

int setLabelKey(TS_ResultSet *r, const char *labelkey) {
    r->labelkey = strdup(labelkey);
    return true;
}

int groupbyLabel(TS_ResultSet *r, char *label) {
    r->groupsType = GroupType_ResultSet;
    r->labelkey = strdup(label);
    return true;
}

int applyReducerToResultSet(TS_ResultSet *r, MultiSeriesReduceOp reducerOp) {
    if (r->groupsType == GroupType_Series) {
        // Labels:
        // <label>=<groupbyvalue>
        // __reducer__=<reducer>
        // __source__=key1,key2,key3
        Label *labels = malloc(sizeof(Label) * 3);
        labels[0].key = RedisModule_CreateStringPrintf(NULL, "%s", r->labelkey);
        labels[0].value = RedisModule_CreateStringPrintf(NULL, "%s", r->labelvalue);
        labels[1].key = RedisModule_CreateStringPrintf(NULL, "__reducer__");
        labels[1].value = RedisModule_CreateStringPrintf(NULL, "max");
        labels[2].key = RedisModule_CreateStringPrintf(NULL, "__source__");
        labels[2].value = RedisModule_CreateStringPrintf(NULL, "");

        const uint64_t total_series = raxSize(r->groups);
        uint64_t at_pos = 0;
        const size_t serie_name_len = strlen(r->labelkey) + strlen(r->labelvalue) + 1;
        char *serie_name = (char *)malloc(serie_name_len * sizeof(char));
        sprintf(serie_name, "%s=%s", r->labelkey, r->labelvalue);
        raxIterator ri;
        raxStart(&ri, r->groups);
        // ^ seek the smallest element of the radix tree.
        raxSeek(&ri, "^", NULL, 0);
        raxNext(&ri);
        // Use the first serie as the initial data
        Series *reduced = (Series *)ri.data;
        RedisModule_StringAppendBuffer(NULL, labels[2].value, ri.key, ri.key_len);
        if (raxRemove(r->groups, ri.key, ri.key_len, NULL)) {
            raxSeek(&ri, ">", ri.key, ri.key_len);
        }
        at_pos++;

        while (raxNext(&ri)) {
            Series *source = (Series *)ri.data;
            MultiSerieReduce(reduced, source, reducerOp);
            if (at_pos > 0 && at_pos < total_series) {
                RedisModule_StringAppendBuffer(NULL, labels[2].value, ",", 1);
            }
            RedisModule_StringAppendBuffer(NULL, labels[2].value, ri.key, ri.key_len);
            at_pos++;
            if (raxRemove(r->groups, ri.key, ri.key_len, NULL)) {
                raxSeek(&ri, ">", ri.key, ri.key_len);
            }
        }
        raxStop(&ri);
        reduced->keyName = RedisModule_CreateStringPrintf(NULL, "%s", serie_name);
        reduced->labels = labels;
        reduced->labelsCount = 3;
        raxInsert(r->groups, (unsigned char *)serie_name, serie_name_len, reduced, reduced);
    } else {
        raxIterator ri;
        raxStart(&ri, r->groups);
        // ^ seek the smallest element of the radix tree.
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri)) {
            TS_ResultSet *innerResultSet = (TS_ResultSet *)ri.data;
            applyReducerToResultSet(innerResultSet, reducerOp);
            // we should now have a serie per label
            // TODO: re-seek and reduce
            // innerResultSet->groupbylabel = NULL;
        }
        raxStop(&ri);
    }
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
        if (r->groupsType == GroupType_ResultSet) {
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
            // replace the serie with the range trimmed one
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
    if (r->groupsType == GroupType_ResultSet) {
        char *labelValue = SeriesGetCStringLabelValue(serie, r->labelkey);
        const size_t labelLen = strlen(labelValue);
        printf("labelValue: %s\n", labelValue);
        TS_ResultSet *labelGroup = raxFind(r->groups, (unsigned char *)labelValue, labelLen);
        if (labelGroup == raxNotFound) {
            labelGroup = createResultSet();
            setLabelKey(labelGroup, r->labelkey);
            setLabelValue(labelGroup, labelValue);
            raxInsert(r->groups, (unsigned char *)labelValue, labelLen, labelGroup, NULL);
            printf("creating new TS_ResultSet for label: %s\n", labelValue);
        }
        addSerieToResultSet(labelGroup, serie, name);
        printf("Total series for label: %s : %d\n", labelValue, raxSize(r->groups));
    } else {
        // If a serie with that name already exists we return
        if (raxFind(r->groups, (unsigned char *)name, namelen) != raxNotFound) {
            return false;
        }
        raxInsert(r->groups, (unsigned char *)name, namelen, serie, NULL);
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
    RedisModule_ReplyWithArray(ctx, raxSize(r->groups));
    raxIterator ri;
    raxStart(&ri, r->groups);
    raxSeek(&ri, "^", NULL, 0);
    Series *s;
    while (raxNext(&ri)) {
        if (r->groupsType == GroupType_ResultSet) {
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
        if (r->groupsType == GroupType_ResultSet) {
            raxFreeWithCallback(r->groups, freeResultSet);
        } else {
            raxFree(r->groups);
        }
    }
    if (r->labelkey)
        free(r->labelkey);
    if (r->labelvalue)
        free(r->labelvalue);
    free(r);
}
