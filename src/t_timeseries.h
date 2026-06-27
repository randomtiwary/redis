/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 * Native time series data type.
 * Timestamps are stored with Facebook Gorilla delta-of-delta bit encoding.
 */

#ifndef __T_TIMESERIES_H
#define __T_TIMESERIES_H

#include <stdint.h>
#include <stddef.h>

/* One sample: value + optional labels. Timestamps live in the Gorilla stream. */
typedef struct tsSample {
    double value;
    char *labels;   /* SDS or NULL */
} tsSample;

/* Gorilla bit buffer for timestamp delta-of-delta encoding. */
typedef struct gorillaTS {
    unsigned char *buf;
    size_t alloc;       /* allocated bytes */
    size_t byte_len;    /* complete bytes written */
    int bit_pos;        /* 0-7 bits used in current partial byte */
    int64_t prev_ts;
    int64_t prev_delta;
    size_t count;       /* number of timestamps encoded */
} gorillaTS;

/* In-memory time series object (robj->ptr). */
typedef struct redisTimeSeries {
    gorillaTS ts;       /* compressed timestamps (sorted ascending) */
    tsSample *samples;  /* parallel array of values/labels, same order */
    size_t len;
    size_t alloc;
    size_t mem_usage;   /* approximate allocation tracker */
} redisTimeSeries;

/* Aggregation kinds for TS.RANGE */
#define TS_AGG_MAX 0
#define TS_AGG_MIN 1
#define TS_AGG_SUM 2
#define TS_AGG_AVG 3

redisTimeSeries *tsCreate(void);
void tsFree(redisTimeSeries *ts);
redisTimeSeries *tsDup(redisTimeSeries *ts);
size_t tsMemUsage(redisTimeSeries *ts);

/* Set or overwrite sample at timestamp. Returns 1 if new, 0 if updated. */
int tsSet(redisTimeSeries *ts, int64_t timestamp, double value, const char *labels);

/* Lookup exact timestamp. Returns 1 and fills *value / *labels (labels may be NULL). */
int tsGet(redisTimeSeries *ts, int64_t timestamp, double *value, const char **labels);

/* Iterate timestamps in [start, end] inclusive; apply aggregation over matching values.
 * Returns number of samples in range (0 if none). On success writes *out_value. */
size_t tsRangeAggregate(redisTimeSeries *ts, int64_t start, int64_t end,
                        int agg, double *out_value);

/* Decode all timestamps into caller-allocated array of length ts->len. */
void tsDecodeTimestamps(redisTimeSeries *ts, int64_t *out);

/* Gorilla helpers (also used by RDB). */
void gorillaInit(gorillaTS *g);
void gorillaReset(gorillaTS *g);
void gorillaFree(gorillaTS *g);
int gorillaAppend(gorillaTS *g, int64_t timestamp);
int gorillaDecodeAll(const gorillaTS *g, int64_t *out, size_t max_out);
size_t gorillaBytes(const gorillaTS *g);

#endif /* __T_TIMESERIES_H */
