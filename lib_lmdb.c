/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   This program is free software.
 *   You can redistribute lessfs and/or modify it under the
 *   terms of either (1) the GNU General Public License;
 *   either version 3 of the License, or (at your option)
 *   any later version as published by the Free Software
 *   Foundation; or (2) obtain a commercial license
 *   by contacting the Author.
 *
 *   This program is distributed in the hope that it will
 *   be useful, but WITHOUT ANY WARRANTY; without even the
 *   implied warranty of MERCHANTABILITY or FITNESS FOR A
 *   PARTICULAR PURPOSE. See the GNU General Public License
 *   for more details.
 */
#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 29
#endif
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#ifdef LMDB

#ifndef LFATAL
#include "lib_log.h"
#endif
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <lmdb.h>
#include <fuse/fuse_lowlevel.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/file.h>
#include <fuse.h>
#include <fcntl.h>
#include <pthread.h>
#include "lib_tc_replacements.h"
#include <stdbool.h>
#include <stdint.h>

#include "lib_safe.h"
#include "lib_cfg.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_common.h"
#include "lib_crypto.h"
#include "file_io.h"
#include "lib_repl.h"
#include "lib_lmdb.h"


/* LMDB environment and database handles */
MDB_env *lmdb_env;
MDB_dbi dbi_dbb;      /* fileblock */
MDB_dbi dbi_dbu;      /* blockusage */
MDB_dbi dbi_dbp;      /* metadata */
MDB_dbi dbi_dbl;      /* hardlink */
MDB_dbi dbi_dbs;      /* symlink */
MDB_dbi dbi_dbdta;    /* blockdata (unused with file_io) */
MDB_dbi dbi_dbdirent; /* directory entries */
MDB_dbi dbi_freelist; /* freelist */

/* Write transaction - single writer model */
MDB_txn *write_txn;

int fdbdta;
int frepl = 0;
int freplbl = 0;
extern unsigned long long nextoffset;
unsigned long long lastoffset;
const char *bdb_lockedby;

static pthread_mutex_t lmdb_mutex = PTHREAD_MUTEX_INITIALIZER;
static const char *lmdb_lockedby = NULL;

#define die_lmdberr(fmt, ...) \
    do { LFATAL(fmt, ##__VA_ARGS__); exit(EXIT_DATAERR); \
    } while (0)

/*
 * Return the MDB_dbi handle for the given database
 * selector constant.
 */
static MDB_dbi lmdb_set_db(int database)
{
    switch (database) {
    case 0: /* DBDTA */
        return dbi_dbdta;
    case 1: /* DBU */
        return dbi_dbu;
    case 2: /* DBB */
        return dbi_dbb;
    case 3: /* DBP */
        return dbi_dbp;
    case 4: /* DBS */
        return dbi_dbs;
    case 5: /* DBL */
        return dbi_dbl;
    case 6: /* FREELIST */
        return dbi_freelist;
    case 7: /* DBDIRENT */
        return dbi_dbdirent;
    default:
        die_lmdberr("lmdb_set_db: invalid database %d",
                     database);
    }
    return dbi_dbp; /* not reached */
}

void bdb_lock(const char *msg)
{
    int ret;
    pthread_mutex_lock(&lmdb_mutex);
    lmdb_lockedby = msg;
    ret = mdb_txn_begin(lmdb_env, NULL, 0, &write_txn);
    if (ret != 0)
        die_lmdberr("bdb_lock txn_begin: %s",
                     mdb_strerror(ret));
}

int try_bdb_lock(void)
{
    int ret = pthread_mutex_trylock(&lmdb_mutex);
    if (ret == 0) {
        int txret;
        lmdb_lockedby = "try_bdb_lock";
        txret = mdb_txn_begin(lmdb_env, NULL, 0,
                               &write_txn);
        if (txret != 0)
            die_lmdberr("try_bdb_lock txn_begin: %s",
                         mdb_strerror(txret));
    }
    return ret;
}

void release_bdb_lock(void)
{
    if (write_txn) {
        int ret = mdb_txn_commit(write_txn);
        write_txn = NULL;
        if (ret != 0)
            die_lmdberr(
                "release_bdb_lock commit: %s",
                mdb_strerror(ret));
    }
    lmdb_lockedby = NULL;
    pthread_mutex_unlock(&lmdb_mutex);
}

void bdb_checkpoint(void)
{
    /* LMDB has no explicit checkpoint; sync env */
    mdb_env_sync(lmdb_env, 1);
}

void bdb_stat(void)
{
    MDB_stat stat;
    if (0 == mdb_env_stat(lmdb_env, &stat)) {
        LINFO("LMDB page_size=%u depth=%u "
              "branch=%zu leaf=%zu overflow=%zu "
              "entries=%zu",
              stat.ms_psize, stat.ms_depth,
              stat.ms_branch_pages, stat.ms_leaf_pages,
              stat.ms_overflow_pages, stat.ms_entries);
    }
}

void start_transactions(void)
{
    int ret;
    FUNC;
    ret = mdb_txn_begin(lmdb_env, NULL, 0, &write_txn);
    if (ret != 0)
        die_lmdberr("txn_begin failed: %s",
                     mdb_strerror(ret));
    EFUNC;
}

void commit_transactions(void)
{
    int ret;
    FUNC;
    ret = mdb_txn_commit(write_txn);
    write_txn = NULL;
    if (ret != 0)
        die_lmdberr("txn_commit failed: %s",
                     mdb_strerror(ret));
    EFUNC;
}

void abort_transactions(void)
{
    FUNC;
    if (write_txn) {
        mdb_txn_abort(write_txn);
        write_txn = NULL;
    }
    /* Per-lock txns: no need to restart;
       release_bdb_lock handles NULL write_txn */
    EFUNC;
}

void bdb_open(void)
{
    int ret;
    struct stat stbuf;
    char *hashstr;
    DAT *data;
    char *sp, *dbname;
    unsigned int db_flags;
    unsigned long long mapsize;

    FUNC;

    /* Set up replication log file - same as BDB */
    if (config->blockdata_io_type == FILE_IO) {
        sp = s_dirname(config->blockdata);
    } else {
        sp = s_strdup(config->blockdata);
    }
    dbname = as_sprintf(__FILE__, __LINE__,
                        "%s/replog.dta", sp);
    config->replication_logfile = s_strdup(dbname);
    if (-1 == (frepl = s_open2(dbname,
            O_CREAT | O_RDWR | O_NOATIME, S_IRWXU)))
        die_syserr();
    if (0 != flock(frepl, LOCK_EX | LOCK_NB)) {
        LFATAL("Failed to lock the replication logfile"
               " %s\nlessfs must be unmounted before"
               " using this option!",
               config->replication_logfile);
        exit(EXIT_USAGE);
    }
    if (config->replication
            && config->replication_role == 0) {
        if (-1 == (freplbl = s_open2(dbname,
                O_RDWR, S_IRWXU)))
            die_syserr();
    }
    s_free(dbname);
    s_free(sp);

    /* Create LMDB environment */
    LDEBUG("Open LMDB environment: %s", config->meta);
    ret = mdb_env_create(&lmdb_env);
    if (ret != 0)
        die_lmdberr("mdb_env_create: %s",
                     mdb_strerror(ret));

    /* Allow 8 named databases */
    ret = mdb_env_set_maxdbs(lmdb_env, 8);
    if (ret != 0)
        die_lmdberr("mdb_env_set_maxdbs: %s",
                     mdb_strerror(ret));

    /* Set map size */
    mapsize = config->lmdb_mapsize
              ? config->lmdb_mapsize
              : LMDB_DEFAULT_MAPSIZE;
    ret = mdb_env_set_mapsize(lmdb_env, mapsize);
    if (ret != 0)
        die_lmdberr("mdb_env_set_mapsize: %s",
                     mdb_strerror(ret));

    /* Open environment */
    ret = mdb_env_open(lmdb_env, config->meta,
                       MDB_NOSYNC | MDB_WRITEMAP, 0644);
    if (ret != 0)
        die_lmdberr("mdb_env_open(%s): %s",
                     config->meta, mdb_strerror(ret));

    /* Start initial transaction to open databases */
    ret = mdb_txn_begin(lmdb_env, NULL, 0, &write_txn);
    if (ret != 0)
        die_lmdberr("initial txn_begin: %s",
                     mdb_strerror(ret));

    db_flags = MDB_CREATE;

    ret = mdb_dbi_open(write_txn, "metadata",
                       db_flags, &dbi_dbp);
    if (ret != 0)
        die_lmdberr("open metadata: %s",
                     mdb_strerror(ret));

    ret = mdb_dbi_open(write_txn, "blockusage",
                       db_flags, &dbi_dbu);
    if (ret != 0)
        die_lmdberr("open blockusage: %s",
                     mdb_strerror(ret));

    ret = mdb_dbi_open(write_txn, "fileblock",
                       db_flags, &dbi_dbb);
    if (ret != 0)
        die_lmdberr("open fileblock: %s",
                     mdb_strerror(ret));

    ret = mdb_dbi_open(write_txn, "hardlink",
                       db_flags | MDB_DUPSORT,
                       &dbi_dbl);
    if (ret != 0)
        die_lmdberr("open hardlink: %s",
                     mdb_strerror(ret));

    ret = mdb_dbi_open(write_txn, "symlink",
                       db_flags, &dbi_dbs);
    if (ret != 0)
        die_lmdberr("open symlink: %s",
                     mdb_strerror(ret));

    ret = mdb_dbi_open(write_txn, "dirent",
                       db_flags | MDB_DUPSORT,
                       &dbi_dbdirent);
    if (ret != 0)
        die_lmdberr("open dirent: %s",
                     mdb_strerror(ret));

    ret = mdb_dbi_open(write_txn, "freelist",
                       db_flags | MDB_DUPSORT,
                       &dbi_freelist);
    if (ret != 0)
        die_lmdberr("open freelist: %s",
                     mdb_strerror(ret));

    ret = mdb_dbi_open(write_txn, "blockdata",
                       db_flags, &dbi_dbdta);
    if (ret != 0)
        die_lmdberr("open blockdata: %s",
                     mdb_strerror(ret));

    /* Commit the DBI-opening transaction */
    ret = mdb_txn_commit(write_txn);
    write_txn = NULL;
    if (ret != 0)
        die_lmdberr("dbi commit: %s",
                     mdb_strerror(ret));

    LINFO("LMDB databases are now open.");

    /* Per-lock transactions: no persistent write_txn */
    write_txn = NULL;
    open_trees();

    if (config->blockdata_io_type == FILE_IO) {
        if (-1 == (fdbdta = s_open2(config->blockdata,
                O_CREAT | O_RDWR | O_NOATIME, S_IRWXU)))
            die_syserr();
        if (-1 == (stat(config->blockdata, &stbuf)))
            die_syserr();
        hashstr = as_sprintf(__FILE__, __LINE__,
                             "NEXTOFFSET");
        config->nexthash = (char *)
            thash((unsigned char *) hashstr,
                  strlen(hashstr));
        s_free(hashstr);
        data = search_dbdata(DBU, config->nexthash,
                             config->hashlen, LOCK);
        if (NULL == data) {
            nextoffset = 0;
            LINFO("First use, nextoffset = 0");
        } else {
            memcpy(&nextoffset, data->data,
                   sizeof(unsigned long long));
            DATfree(data);
        }
        LINFO("nextoffset = %llu", nextoffset);
        lastoffset = nextoffset;
    }

    EFUNC;
}

void bdb_close(void)
{
    FUNC;
    if (write_txn) {
        mdb_txn_commit(write_txn);
        write_txn = NULL;
    }
    close_trees();
    if (config->blockdata_io_type == FILE_IO) {
        fsync(fdbdta);
        close(fdbdta);
        free(config->nexthash);
    }
    flock(frepl, LOCK_UN);
    fsync(frepl);
    close(frepl);
    if (config->replication
            && config->replication_role == 0)
        close(freplbl);
    s_free(config->replication_logfile);

    mdb_dbi_close(lmdb_env, dbi_dbb);
    mdb_dbi_close(lmdb_env, dbi_dbu);
    mdb_dbi_close(lmdb_env, dbi_dbp);
    mdb_dbi_close(lmdb_env, dbi_dbl);
    mdb_dbi_close(lmdb_env, dbi_dbs);
    mdb_dbi_close(lmdb_env, dbi_dbdta);
    mdb_dbi_close(lmdb_env, dbi_dbdirent);
    mdb_dbi_close(lmdb_env, dbi_freelist);
    mdb_env_close(lmdb_env);
    lmdb_env = NULL;
    EFUNC;
}

void drop_databases(void)
{
    MDB_txn *drop_txn;
    int ret;

    FUNC;
    if (write_txn) {
        mdb_txn_commit(write_txn);
        write_txn = NULL;
    }
    ret = mdb_txn_begin(lmdb_env, NULL, 0, &drop_txn);
    if (ret != 0)
        die_lmdberr("drop txn_begin: %s",
                     mdb_strerror(ret));
    mdb_drop(drop_txn, dbi_dbb, 0);
    mdb_drop(drop_txn, dbi_dbu, 0);
    mdb_drop(drop_txn, dbi_dbp, 0);
    mdb_drop(drop_txn, dbi_dbl, 0);
    mdb_drop(drop_txn, dbi_dbs, 0);
    mdb_drop(drop_txn, dbi_dbdta, 0);
    mdb_drop(drop_txn, dbi_dbdirent, 0);
    mdb_drop(drop_txn, dbi_freelist, 0);
    ret = mdb_txn_commit(drop_txn);
    if (ret != 0)
        die_lmdberr("drop commit: %s",
                     mdb_strerror(ret));
    LINFO("All LMDB databases dropped.");
    EFUNC;
}

DAT *search_dbdata(int database, void *keydata,
                   int len, bool lock)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);

    dbi = lmdb_set_db(database);


    mkey.mv_data = keydata;
    mkey.mv_size = len;
    ret = mdb_get(write_txn, dbi, &mkey, &mdata);
    if (ret == 0) {
        result = s_malloc(sizeof(DAT));
        result->data = s_malloc(mdata.mv_size);
        memcpy(result->data, mdata.mv_data,
               mdata.mv_size);
        result->size = mdata.mv_size;
    }

    if (lock)
        release_bdb_lock();
    EFUNC;
    return result;
}

void delete_key(int database, void *keydata,
                int len, const char *msg)
{
    MDB_dbi dbi;
    MDB_val mkey;
    int ret;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(database);
    mkey.mv_data = keydata;
    mkey.mv_size = len;
    ret = mdb_del(write_txn, dbi, &mkey, NULL);
    if (ret != 0 && ret != MDB_NOTFOUND) {
        if (msg)
            die_lmdberr("delete_key failed: %s",
                         mdb_strerror(ret));
        else
            LINFO("delete_key failed: %s",
                  mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLDELETE,
                        keydata, len, NULL, 0,
                        MAX_ALLOWED_THREADS - 2);
    }
}

void bin_write_dbdata(int database, void *keydata,
                      int keylen, void *valdata,
                      int datalen)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn, dbi, &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn);
        write_txn = NULL;
        LFATAL("bin_write_dbdata : database %u "
               "keylen %u datalen %u",
               database, keylen, datalen);
        die_lmdberr("Database write failed: %s",
                     mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void bin_write(int database, void *keydata, int keylen,
               void *valdata, int datalen)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn, dbi, &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn);
        write_txn = NULL;
        die_lmdberr("bin_write failed: %s",
                     mdb_strerror(ret));
    }
    release_bdb_lock();
}

void start_flush_commit(void)
{
    FUNC;
    if (config->transactions)
        lessfs_trans_stamp();
    if (lastoffset != nextoffset) {
        LDEBUG("write nextoffset=%llu", nextoffset);
        bin_write_dbdata(DBU, config->nexthash,
                         config->hashlen,
                         (unsigned char *) &nextoffset,
                         sizeof(unsigned long long));
        lastoffset = nextoffset;
    }
    sync_all_filesizes();
    if (config->blockdata_io_type == FILE_IO) {
        fsync(fdbdta);
    }
    /* Per-lock txns: each operation already committed.
       Sync LMDB env to disk for durability. */
    mdb_env_sync(lmdb_env, 1);
    EFUNC;
}

void end_flush_commit(void)
{
    FUNC;
    if (config->transactions) {
        /* Per-lock txns: sync env for durability */
        mdb_env_sync(lmdb_env, 1);
    }
    EFUNC;
}


/* === Btree / cursor functions === */

void btbin_write_dbdata(int database, void *keydata,
                        int keylen, void *valdata,
                        int datalen)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn, dbi, &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn);
        write_txn = NULL;
        die_lmdberr("btbin_write_dbdata failed: %s",
                     mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void btbin_write_dup(int database, void *keydata,
                     int keylen, void *valdata,
                     int datalen, bool lock)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    /* LMDB DUPSORT databases accept dups natively */
    ret = mdb_put(write_txn, dbi, &mkey, &mdata, 0);
    if (database == DBDIRENT && keylen == sizeof(unsigned long long) && datalen == sizeof(unsigned long long)) {
        unsigned long long trc_key, trc_val;
        memcpy(&trc_key, keydata, sizeof(trc_key));
        memcpy(&trc_val, valdata, sizeof(trc_val));
    }
    if (ret != 0) {
        mdb_txn_abort(write_txn);
        write_txn = NULL;
        die_lmdberr("btbin_write_dup failed: %s",
                     mdb_strerror(ret));
    }
    if (lock)
        release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

int bt_entry_exists(int database, void *keydata,
                    int keylen, void *valdata,
                    int datalen)
{
    MDB_dbi dbi;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, found = 0;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(database);

    ret = mdb_cursor_open(write_txn, dbi, &cur);
    if (ret != 0) {
        release_bdb_lock();
        die_lmdberr("bt_entry_exists cursor: %s",
                     mdb_strerror(ret));
    }
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_GET_BOTH);
    if (ret == 0)
        found = 1;
    mdb_cursor_close(cur);
    release_bdb_lock();
    EFUNC;
    return found;
}

DAT *btsearch_keyval(int database, void *keydata,
                     int keylen, void *val,
                     int vallen, bool lock)
{
    MDB_dbi dbi;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(database);

    ret = mdb_cursor_open(write_txn, dbi, &cur);
    if (ret != 0) {
        if (lock) release_bdb_lock();
        die_lmdberr("btsearch_keyval cursor: %s",
                     mdb_strerror(ret));
    }
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = val;
    mdata.mv_size = vallen;

    if (val == NULL || vallen == 0) {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_SET_KEY);
    } else {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_GET_BOTH_RANGE);
    }
    if (ret == 0) {
        /*
         * Verify the value starts with the
         * requested prefix.
         */
        if (vallen == 0
                || (mdata.mv_size >= (size_t) vallen
                && 0 == memcmp(mdata.mv_data,
                               val, vallen))) {
            result = s_malloc(sizeof(DAT));
            result->data = s_malloc(mdata.mv_size);
            memcpy(result->data, mdata.mv_data,
                   mdata.mv_size);
            result->size = mdata.mv_size;
        }
    }
    mdb_cursor_close(cur);
    if (lock)
        release_bdb_lock();
    EFUNC;
    return result;
}

int btdelete_curkey(int database, void *keydata,
                    int keylen, void *value,
                    int vallen, const char *msg)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    if (database == DBDIRENT && keylen == sizeof(unsigned long long) && vallen == sizeof(unsigned long long)) {
        unsigned long long trc_key, trc_val;
        memcpy(&trc_key, keydata, sizeof(trc_key));
        memcpy(&trc_val, value, sizeof(trc_val));
    }
    dbi = lmdb_set_db(database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = value;
    mdata.mv_size = vallen;
    /* mdb_del with both key+data deletes specific dup */
    ret = mdb_del(write_txn, dbi, &mkey, &mdata);
    if (ret != 0 && ret != MDB_NOTFOUND) {
        if (msg)
            LFATAL("btdelete_curkey %s: %s",
                   msg, mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLDELETECURKEY,
                        keydata, keylen,
                        value, vallen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
    return (ret == 0) ? 0 : -1;
}


/* === Complex cursor functions === */

DDSTAT *dnode_bname_to_inode(void *dinode, int dlen,
                             char *bname)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    DAT *statdata;
    DDSTAT *filestat = NULL, *lfilestat;
    DAT *fname;
    DINOINO dinoino;
    unsigned long long *valnode;
    unsigned long long inode;
#ifdef ENABLE_CRYPTO
    DAT *decrypted;
#endif

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn, dbi_dbdirent, &cur);
    if (ret != 0) {
        release_bdb_lock();
        return NULL;
    }

    memcpy(&inode, dinode, sizeof(unsigned long long));
    mkey.mv_data = &inode;
    mkey.mv_size = sizeof(unsigned long long);
    mdata.mv_size = 0;
    mdata.mv_data = NULL;

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0) {
        LDEBUG("dnode_bname_to_inode: not found %s",
               mdb_strerror(ret));
        goto end;
    }
    do {
        valnode = (unsigned long long *) mdata.mv_data;
        if (inode == *valnode && inode != 1)
            continue;
        LDEBUG("Search for inode %llu", *valnode);
        statdata = search_dbdata(DBP, valnode,
                       sizeof(unsigned long long),
                       NOLOCK);
        if (NULL == statdata) {
            LINFO("Unable to find file in dbp.");
            continue;
        }
#ifdef ENABLE_CRYPTO
        if (config->encryptmeta
                && config->encryptdata) {
            decrypted = lfsdecrypt(statdata);
            DATfree(statdata);
            statdata = decrypted;
        }
#endif
        filestat = (DDSTAT *) statdata->data;
        if (0 != filestat->filename[0]) {
            s_free(statdata);
            LDEBUG("compare bname %s with %s",
                   bname, filestat->filename);
            if (0 == strcmp(bname,
                            filestat->filename))
            {
                break;
            }
        } else {
            memcpy(&dinoino.dirnode, dinode,
                   sizeof(unsigned long long));
            dinoino.inode = filestat->stbuf.st_ino;
            fname = btsearch_keyval(DBL, &dinoino,
                        sizeof(DINOINO), bname,
                        strlen(bname), NOLOCK);
            if (fname) {
                lfilestat =
                    s_zmalloc(sizeof(DDSTAT));
                memcpy(lfilestat, statdata->data,
                       statdata->size);
                s_free(statdata);
                ddstatfree(filestat);
                memcpy(&lfilestat->filename,
                       fname->data, fname->size);
                lfilestat->filename[fname->size] = 0;
                filestat = lfilestat;
                DATfree(fname);
                break;
            } else {
                s_free(statdata);
            }
        }
        ddstatfree(filestat);
        filestat = NULL;
    } while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                                 MDB_NEXT_DUP));
end:
    mdb_cursor_close(cur);
    release_bdb_lock();
    if (filestat) {
        LDEBUG("dnode_bname_to_inode: %s inode %llu",
               filestat->filename,
               (unsigned long long)
               filestat->stbuf.st_ino);
    }
    return filestat;
}

unsigned long long has_nodes(unsigned long long inode)
{
    unsigned long long res = 0;
    unsigned long long *filenode;
    DAT *filedata;
    bool dotdir = 0;
    DDSTAT *ddstat;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn, dbi_dbdirent, &cur);
    if (ret != 0) {
        release_bdb_lock();
        return 0;
    }

    mkey.mv_data = &inode;
    mkey.mv_size = sizeof(inode);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0)
        goto end;
    do {
        filenode =
            (unsigned long long *) mdata.mv_data;
        filedata = search_dbdata(DBP, filenode,
                       sizeof(unsigned long long),
                       NOLOCK);
        if (filedata) {
            ddstat = value_to_ddstat(filedata);
            DATfree(filedata);
            if (*filenode == inode)
                dotdir = 1;
            if (ddstat->filename) {
                if (0 == strcmp(ddstat->filename, "."))
                    dotdir = 1;
                if (0 == strcmp(ddstat->filename, ".."))
                    dotdir = 1;
            }
            if (!dotdir) {
                res++;
            }
            ddstatfree(ddstat);
            dotdir = 0;
        }
    } while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                                 MDB_NEXT_DUP));
end:
    mdb_cursor_close(cur);
    release_bdb_lock();
    LDEBUG("has_nodes inode %llu contains %llu files",
           inode, res);
    EFUNC;
    return res;
}

int fs_readdir(const char *path, void *buf,
               fuse_fill_dir_t filler, off_t offset,
               struct fuse_file_info *fi)
{
    int retcode = 0;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    struct stat stbuf;
    DAT *filedata;
    DDSTAT *ddstat;

    FUNC;
    (void) offset;
    (void) fi;

    ret = dbstat(path, &stbuf, 1);
    if (0 != ret)
        return -ENOENT;
    if (0 == strcmp(path, "/.lessfs/locks"))
        locks_to_dir(buf, filler, fi);

    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn, dbi_dbdirent, &cur);
    if (ret != 0) {
        release_bdb_lock();
        return -EIO;
    }

    mkey.mv_data = &stbuf.st_ino;
    mkey.mv_size = sizeof(unsigned long long);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            if (0 != memcmp(mdata.mv_data,
                            &stbuf.st_ino,
                            sizeof(unsigned long long)))
            {
                filedata = search_dbdata(DBP,
                               mdata.mv_data,
                               mdata.mv_size, NOLOCK);
                if (filedata) {
                    ddstat =
                        value_to_ddstat(filedata);
                    DATfree(filedata);
                    if (ddstat->filename[0] == 0) {
                        fs_read_hardlink(stbuf,
                            ddstat, buf, filler, fi);
                    } else {
                        fil_fuse_info(ddstat, buf,
                                      filler, fi);
                    }
                    ddstatfree(ddstat);
                }
            }
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    release_bdb_lock();
    LDEBUG("fs_readdir: return");
    return retcode;
}

void fs_read_hardlink(struct stat stbuf,
                      DDSTAT *ddstat, void *buf,
                      fuse_fill_dir_t filler,
                      struct fuse_file_info *fi)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    DINOINO dinoino;
    int ret;

    FUNC;
    dinoino.dirnode = stbuf.st_ino;
    dinoino.inode = ddstat->stbuf.st_ino;

    ret = mdb_cursor_open(write_txn, dbi_dbl, &cur);
    if (ret != 0) {
        return;
    }

    mkey.mv_data = &dinoino;
    mkey.mv_size = sizeof(DINOINO);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            memcpy(&ddstat->filename, mdata.mv_data,
                   mdata.mv_size);
            ddstat->filename[mdata.mv_size] = 0;
            LDEBUG("fs_read_hardlink: %s size %zu",
                   ddstat->filename, mdata.mv_size);
            fil_fuse_info(ddstat, buf, filler, fi);
            ddstat->filename[0] = 0;
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    EFUNC;
}

int count_dirlinks(void *linkstr, int len)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int count = 0, ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn, dbi_dbl, &cur);
    if (ret != 0) {
        release_bdb_lock();
        return 0;
    }

    mkey.mv_data = linkstr;
    mkey.mv_size = len;
    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0)
        goto end_exit;
    do {
        count++;
        if (count > 1)
            break;
    } while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                                 MDB_NEXT_DUP));
end_exit:
    mdb_cursor_close(cur);
    release_bdb_lock();
    EFUNC;
    return count;
}

unsigned long long get_offset_fast(
    unsigned long long mbytes)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    bool found = 0;
    FREEBLOCK *freeblock = NULL;
    time_t thetime;
    unsigned long long offset = 0;
    unsigned long long fsize;

    FUNC;
    thetime = time(NULL);
    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn, dbi_freelist,
                          &cur);
    if (ret != 0) {
        release_bdb_lock();
        return 0;
    }

    mkey.mv_data = &mbytes;
    mkey.mv_size = sizeof(mbytes);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            if (mdata.mv_size < sizeof(FREEBLOCK)) {
                memcpy(&offset, mdata.mv_data,
                       mdata.mv_size);
            } else {
                freeblock =
                    s_malloc(sizeof(FREEBLOCK));
                memcpy(freeblock, mdata.mv_data,
                       mdata.mv_size);
                if (freeblock->reuseafter > thetime) {
                    s_free(freeblock);
                    freeblock = NULL;
                    continue;
                }
                offset = freeblock->offset;
            }
            memcpy(&fsize, mkey.mv_data, mkey.mv_size);
            mdb_cursor_del(cur, 0);
            found = 1;
            break;
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    release_bdb_lock();
    if (found) {
        if (config->replication == 1
                && config->replication_role == 0) {
            if (freeblock) {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &fsize,
                    sizeof(unsigned long long),
                    (char *) freeblock,
                    sizeof(FREEBLOCK),
                    MAX_ALLOWED_THREADS - 2);
            } else {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &fsize,
                    sizeof(unsigned long long),
                    (char *) &offset,
                    sizeof(unsigned long long),
                    MAX_ALLOWED_THREADS - 2);
            }
        }
        if (freeblock)
            s_free(freeblock);
        EFUNC;
        return offset;
    }
    if (freeblock)
        s_free(freeblock);
    EFUNC;
    return 0;
}

INUSE *get_offset_reclaim(unsigned long long mbytes,
                          unsigned long long offset)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    FREEBLOCK *freeblock = NULL;
    time_t thetime;
    bool hasone = 0;
    unsigned long long asize;
    INUSE *inuse = NULL;

    FUNC;
    thetime = time(NULL);
    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn, dbi_freelist,
                          &cur);
    if (ret != 0) {
        release_bdb_lock();
        return NULL;
    }

    mkey.mv_data = &mbytes;
    mkey.mv_size = sizeof(mbytes);

    LDEBUG("get_offset_reclaim: search %llu blocks",
           mbytes);

    while (1) {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_SET_RANGE);
        while (ret == 0) {
            if (mkey.mv_size == strlen("TRNCTD")) {
                ret = mdb_cursor_get(cur, &mkey,
                          &mdata, MDB_NEXT);
                continue;
            }
            memcpy(&asize, mkey.mv_data,
                   sizeof(unsigned long long));
            if (asize < mbytes) {
                ret = mdb_cursor_get(cur, &mkey,
                          &mdata, MDB_NEXT);
                continue;
            }
            if (mdata.mv_size < sizeof(FREEBLOCK)) {
                memcpy(&offset, mdata.mv_data,
                       mdata.mv_size);
            } else {
                freeblock =
                    s_malloc(sizeof(FREEBLOCK));
                memcpy(freeblock, mdata.mv_data,
                       mdata.mv_size);
                if (freeblock->reuseafter > thetime
                        && hasone == 0) {
                    s_free(freeblock);
                    freeblock = NULL;
                    hasone = 1;
                    ret = mdb_cursor_get(cur, &mkey,
                              &mdata, MDB_NEXT);
                    continue;
                }
                if (freeblock->reuseafter > thetime
                        && hasone == 1)
                    LINFO("get_offset_reclaim:"
                          " early space reclaim");
                offset = freeblock->offset;
            }
            inuse = s_zmalloc(sizeof(INUSE));
            inuse->allocated_size = asize * 512;
            inuse->offset = offset;
            mdb_cursor_del(cur, 0);
            break;
        }
        if (inuse)
            break;
        if (!hasone)
            break;
        /* Retry from beginning */
        mkey.mv_data = &mbytes;
        mkey.mv_size = sizeof(mbytes);
        hasone = 0;
    }
    mdb_cursor_close(cur);
    release_bdb_lock();
    if (inuse) {
        if (config->replication == 1
                && config->replication_role == 0) {
            if (freeblock) {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &asize,
                    sizeof(unsigned long long),
                    (char *) freeblock,
                    sizeof(FREEBLOCK),
                    MAX_ALLOWED_THREADS - 2);
            } else {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &asize,
                    sizeof(unsigned long long),
                    (char *) &offset,
                    sizeof(unsigned long long),
                    MAX_ALLOWED_THREADS - 2);
            }
        }
    }
    if (freeblock)
        s_free(freeblock);
    return inuse;
}

void bdb_restart_truncation(void)
{
    char *keystr = "TRNCTD";
    pthread_t truncate_thread;
    struct truncate_thread_data *trunc_data;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    if (!config->background_delete)
        return;

    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn, dbi_freelist,
                          &cur);
    if (ret != 0) {
        release_bdb_lock();
        return;
    }

    mkey.mv_data = keystr;
    mkey.mv_size = strlen(keystr);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0)
        goto end_exit;
    do {
        trunc_data = s_malloc(mdata.mv_size);
        memcpy(trunc_data, mdata.mv_data,
               mdata.mv_size);
        if (0 == trunc_data->inode) {
            s_free(trunc_data);
            break;
        }
        LINFO("Resume truncation of inode %llu",
              trunc_data->inode);
        create_inode_note(trunc_data->inode);
        if (0 != pthread_create(&truncate_thread,
                NULL, file_truncate_worker,
                (void *) trunc_data))
            die_syserr();
        if (0 != pthread_detach(truncate_thread))
            die_syserr();
    } while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                                 MDB_NEXT_DUP));
end_exit:
    mdb_cursor_close(cur);
    release_bdb_lock();
    EFUNC;
}


/* === List / diagnostic functions === */

char *lessfs_stats()
{
    char *lfsmsg = NULL;
    char *line;
    DDSTAT *ddstat;
    DAT data;
    unsigned long long inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;
    const char **lines = NULL;
    int count = 1;
    unsigned int lcount = 0;
    float ratio;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    MDB_stat mst;
    int ret;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    mdb_stat(write_txn, dbi_dbp, &mst);
    lcount = mst.ms_entries;
    lcount++;
    lines = s_malloc(lcount * sizeof(char *));
    lines[0] =
        as_sprintf(__FILE__, __LINE__,
            "  INODE             SIZE"
            "  COMPRESSED_SIZE"
            "            RATIO FILENAME\n");

    ret = mdb_cursor_open(write_txn, dbi_dbp, &cur);
    if (ret != 0) {
        release_bdb_lock();
        s_free((char *) lines[0]);
        s_free(lines);
        return s_strdup("Error: cannot open cursor");
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        if (0 != memcmp(mkey.mv_data, nfi, 3)
                && 0 != memcmp(mkey.mv_data,
                               seq, 3)) {
            memcpy(&inode, mkey.mv_data,
                   sizeof(unsigned long long));
            data.data = mdata.mv_data;
            data.size = mdata.mv_size;
            if (inode == 0) {
                crypto = (CRYPTO *) mdata.mv_data;
            } else {
                ddstat = value_to_ddstat(&data);
                ratio = 0;
                if (ddstat->stbuf.st_size != 0) {
                    if (ddstat->real_size == 0) {
                        ratio = 1000;
                    } else {
                        ratio =
                            (float)
                            ddstat->stbuf.st_size
                            / (float)
                            ddstat->real_size;
                    }
                }
                if (S_ISREG(
                        ddstat->stbuf.st_mode)) {
#ifdef x86_64
                    line = as_sprintf(
                        __FILE__, __LINE__,
                        "%7lu  %15lu"
                        "  %15llu"
                        "  %15.2f %s\n",
                        ddstat->stbuf.st_ino,
                        ddstat->stbuf.st_size,
                        ddstat->real_size,
                        ratio,
                        ddstat->filename);
#else
                    line = as_sprintf(
                        __FILE__, __LINE__,
                        "%7llu  %15llu"
                        "  %15llu"
                        "  %15.2f %s\n",
                        ddstat->stbuf.st_ino,
                        ddstat->stbuf.st_size,
                        ddstat->real_size,
                        ratio,
                        ddstat->filename);
#endif
                    lines[count++] = line;
                } else {
                    lcount--;
                }
                ddstatfree(ddstat);
            }
        } else {
            lcount--;
        }
        if (count == lcount)
            break;
    }
    lfsmsg = as_strarrcat(lines, count);
    mdb_cursor_close(cur);
    while (count) {
        s_free((char *) lines[--count]);
    }
    s_free(lines);
    release_bdb_lock();
    EFUNC;
    return lfsmsg;
}

void listdbp()
{
    DDSTAT *ddstat;
    DAT *data;
    unsigned long long *inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int count = 0, ret;

    ret = mdb_cursor_open(write_txn, dbi_dbp, &cur);
    if (ret != 0) {
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        count++;
        inode =
            (unsigned long long *) mkey.mv_data;
        if (0 == memcmp(mkey.mv_data, nfi, 3)
                || 0 == memcmp(mkey.mv_data, seq, 3))
        {
            inode =
                (unsigned long long *) mdata.mv_data;
            printf("NFI : %llu\n", *inode);
        } else {
            data = search_dbdata(DBP, inode,
                       sizeof(unsigned long long),
                       NOLOCK);
            if (data) {
                if (*inode == 0) {
                    crypto = (CRYPTO *) data->data;
                } else {
                    ddstat =
                        value_to_ddstat(data);
#ifdef x86_64
                    printf(
                        "ddstat->filename %s \n"
                        "      ->inode %lu  "
                        "-> size %lu  "
                        "-> real_size %llu "
                        "time %lu mode %u\n",
                        ddstat->filename,
                        ddstat->stbuf.st_ino,
                        ddstat->stbuf.st_size,
                        ddstat->real_size,
                        ddstat->stbuf.st_atim.tv_sec,
                        ddstat->stbuf.st_mode);
#else
                    printf(
                        "ddstat->filename %s \n"
                        "      ->inode %llu "
                        "-> size %llu "
                        "-> real_size %llu "
                        "time %lu mode %u\n",
                        ddstat->filename,
                        ddstat->stbuf.st_ino,
                        ddstat->stbuf.st_size,
                        ddstat->real_size,
                        ddstat->stbuf.st_atim.tv_sec,
                        ddstat->stbuf.st_mode);
#endif
                    if (S_ISDIR(
                            ddstat->stbuf.st_mode)) {
                        printf(
                            "      ->filename %s"
                            " is a directory\n",
                            ddstat->filename);
                    }
                    ddstatfree(ddstat);
                }
                DATfree(data);
            }
        }
    }
    mdb_cursor_close(cur);
}

void listdirent()
{
    unsigned long long *dir;
    unsigned long long *ent;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    ret = mdb_cursor_open(write_txn, dbi_dbdirent, &cur);
    if (ret != 0) {
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        dir =
            (unsigned long long *) mkey.mv_data;
        ent =
            (unsigned long long *) mdata.mv_data;
        printf("%llu:%llu\n", *dir, *ent);
    }
    mdb_cursor_close(cur);
}

void list_hardlinks()
{
    unsigned long long inode;
    DINOINO dinoino;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    ret = mdb_cursor_open(write_txn, dbi_dbl, &cur);
    if (ret != 0) {
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        if (mkey.mv_size == sizeof(DINOINO)) {
            memcpy(&dinoino, mkey.mv_data,
                   sizeof(DINOINO));
            printf("dinoino %llu-%llu : inode %s\n",
                   dinoino.dirnode, dinoino.inode,
                   (char *) mdata.mv_data);
        } else {
            memcpy(&inode, mkey.mv_data,
                   sizeof(unsigned long long));
            memcpy(&dinoino, mdata.mv_data,
                   sizeof(DINOINO));
            printf("inode %llu : %llu-%llu "
                   "dinoino\n", inode,
                   dinoino.dirnode, dinoino.inode);
        }
    }
    mdb_cursor_close(cur);
}

void listdbb()
{
    char *asc_hash = NULL;
    unsigned long long inode;
    unsigned long long blocknr;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    ret = mdb_cursor_open(write_txn, dbi_dbb, &cur);
    if (ret != 0) {
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        asc_hash = ascii_hash(
            (unsigned char *) mdata.mv_data);
        memcpy(&inode, mkey.mv_data,
               sizeof(unsigned long long));
        memcpy(&blocknr,
               mkey.mv_data
                   + sizeof(unsigned long long),
               sizeof(unsigned long long));
        printf("%llu-%llu : %s\n",
               inode, blocknr, asc_hash);
        s_free(asc_hash);
    }
    mdb_cursor_close(cur);
}

void listfree(int freespace_summary)
{
    unsigned long long mbytes;
    unsigned long long offset;
    unsigned long freespace = 0;
    struct truncate_thread_data *trunc_data;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    FREEBLOCK *freeblock;
    int ret;

    ret = mdb_cursor_open(write_txn, dbi_freelist, &cur);
    if (ret != 0) {
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        if (mkey.mv_size == strlen("TRNCTD")) {
            trunc_data =
                (struct truncate_thread_data *)
                mdata.mv_data;
            printf("Truncation not finished for "
                   "inode %llu : start %llu "
                   "-> end %llu size %llu\n",
                   trunc_data->inode,
                   trunc_data->blocknr,
                   trunc_data->lastblocknr,
                   (unsigned long long)
                   trunc_data->stbuf.st_size);
        } else {
            memcpy(&mbytes, mkey.mv_data,
                   sizeof(unsigned long long));
            freespace += (mbytes * 512);
            if (mdata.mv_size
                    < sizeof(FREEBLOCK)) {
                memcpy(&offset, mdata.mv_data,
                   sizeof(unsigned long long));
                if (!freespace_summary) {
                    printf("offset = %llu :"
                           " blocks = %llu :"
                           " bytes = %llu\n",
                           offset, mbytes,
                           mbytes * 512);
                }
            } else {
                freeblock =
                    (FREEBLOCK *) mdata.mv_data;
                if (!freespace_summary) {
                    printf("offset = %llu :"
                           " blocks = %llu :"
                           " bytes = %llu"
                           " reuseafter %lu\n",
                           freeblock->offset,
                           mbytes, mbytes * 512,
                           freeblock->reuseafter);
                }
            }
        }
    }
    printf("Total available space in %s : %lu\n\n",
           config->blockdata, freespace);
    mdb_cursor_close(cur);
}

void listdbu()
{
}

void listdta()
{
}

void list_symlinks()
{
}

void flistdbu()
{
    char *asc_hash = NULL;
    INUSE *inuse;
    unsigned long long nexto;
    unsigned long rsize;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    ret = mdb_cursor_open(write_txn, dbi_dbu, &cur);
    if (ret != 0) {
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        if (config->nexthash && 0 == memcmp(config->nexthash,
                        mkey.mv_data,
                        config->hashlen)) {
            memcpy(&nexto, mdata.mv_data,
                   sizeof(unsigned long long));
            printf("\nnextoffset = %llu\n\n", nexto);
        } else {
            inuse = (INUSE *) mdata.mv_data;
            asc_hash = ascii_hash(
                (unsigned char *) mkey.mv_data);
            printf("%s   : %llu\n",
                   asc_hash, inuse->inuse);
            printf("offset"
                   "                 "
                   "              : %llu\n",
                   inuse->offset);
            printf("size"
                   "                 "
                   "        : %lu\n",
                   inuse->size);
            rsize = inuse->allocated_size;
            printf("allocated size"
                   "                 "
                   "  : %lu\n\n", rsize);
            s_free(asc_hash);
            if (inuse->allocated_size - 512
                    > inuse->size)
                printf("Reclaimed oversized"
                       " %lu - %llu\n",
                       inuse->size,
                       inuse->allocated_size);
        }
    }
    mdb_cursor_close(cur);
}


/*
 * inode_to_path: reconstruct the full path for an
 * inode by walking up the directory tree via DBDIRENT.
 * Returns a freshly allocated path string, or NULL
 * if not found.  Caller must s_free() the result.
 *
 * Works by: for each inode, look up its DDSTAT to get
 * the filename, then find which parent directory
 * contains it via DBDIRENT reverse lookup.
 */
char *inode_to_path(unsigned long long inode)
{
    char *components[256];
    int depth = 0;
    unsigned long long cur_ino = inode;
    DAT *dskdata;
    DDSTAT *ddstat;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    unsigned long long parent_ino;
    int found;
    char *result;
    int total_len;
    int idx;

    FUNC;
    /* Root inode is always 1 */
    if (cur_ino == 1)
        return s_strdup("/");

    bdb_lock((char *) __PRETTY_FUNCTION__);

    while (cur_ino != 1 && depth < 255) {
        /* Get the filename for cur_ino */
        dskdata = search_dbdata(DBP, &cur_ino,
            sizeof(unsigned long long), NOLOCK);
        if (dskdata == NULL) {
            release_bdb_lock();
            goto fail;
        }
        ddstat = value_to_ddstat(dskdata);
        DATfree(dskdata);

        if (strcmp(ddstat->filename, "/") == 0) {
            ddstatfree(ddstat);
            break;  /* reached root */
        }
        if (ddstat->filename[0] == 0) {
            /* Hardlinked file: empty DBP filename.
               Look up a name via DBL. */
            DAT *lnk = btsearch_keyval(DBL,
                &cur_ino,
                sizeof(unsigned long long),
                NULL, 0, NOLOCK);
            if (lnk) {
                DINOINO dino;
                DAT *nm;
                memcpy(&dino, lnk->data,
                       lnk->size);
                DATfree(lnk);
                nm = btsearch_keyval(DBL,
                    &dino, sizeof(DINOINO),
                    NULL, 0, NOLOCK);
                if (nm) {
                    components[depth] =
                        s_zmalloc(nm->size + 1);
                    memcpy(components[depth],
                           nm->data, nm->size);
                    depth++;
                    DATfree(nm);
                    ddstatfree(ddstat);
                } else {
                    ddstatfree(ddstat);
                    release_bdb_lock();
                    goto fail;
                }
            } else {
                ddstatfree(ddstat);
                release_bdb_lock();
                goto fail;
            }
        } else {
            components[depth] =
                s_strdup(ddstat->filename);
            depth++;
            ddstatfree(ddstat);
        }

        /*
         * Find the parent: scan DBDIRENT looking
         * for a directory that contains cur_ino as
         * a child.
         */
        found = 0;
        ret = mdb_cursor_open(write_txn,
                              dbi_dbdirent, &cur);
        if (ret != 0) {
            release_bdb_lock();
            goto fail;
        }

        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_FIRST);
        while (ret == 0) {
            if (mdata.mv_size
                == sizeof(unsigned long long)) {
                unsigned long long child_ino;
                memcpy(&child_ino, mdata.mv_data,
                       sizeof(unsigned long long));
                if (child_ino == cur_ino) {
                    unsigned long long key_ino;
                    memcpy(&key_ino, mkey.mv_data,
                        sizeof(unsigned long long));
                    if (key_ino != cur_ino) {
                        parent_ino = key_ino;
                        found = 1;
                        break;
                    }
                }
            }
            ret = mdb_cursor_get(cur, &mkey, &mdata,
                                 MDB_NEXT);
        }
        mdb_cursor_close(cur);

        if (!found) {
            /* Probably reached root indirectly */
            break;
        }
        cur_ino = parent_ino;
    }

    release_bdb_lock();

    if (depth == 0)
        return s_strdup("/");

    /* Build path from components (reverse order) */
    total_len = 1;  /* leading '/' */
    for (idx = depth - 1; idx >= 0; idx--)
        total_len += strlen(components[idx]) + 1;

    result = s_malloc(total_len + 1);
    result[0] = 0;
    for (idx = depth - 1; idx >= 0; idx--) {
        strcat(result, "/");
        strcat(result, components[idx]);
        s_free(components[idx]);
    }

    EFUNC;
    return result;

fail:
    for (idx = 0; idx < depth; idx++)
        s_free(components[idx]);
    return NULL;
}

/*
 * fs_readdir_ll: low-level readdir for the LMDB
 * backend.  Fills buf using fuse_add_direntry().
 * Returns the number of bytes used in buf.
 */
size_t fs_readdir_ll(fuse_req_t req,
                     unsigned long long dir_ino,
                     char *buf, size_t bufsize,
                     off_t off)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    DAT *filedata;
    DDSTAT *ddstat;
    struct stat stbuf;
    size_t buf_used = 0;
    size_t entsize;
    off_t idx = 0;
    char *bname;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn,
                          dbi_dbdirent, &cur);
    if (ret != 0) {
        release_bdb_lock();
        return 0;
    }

    mkey.mv_data = &dir_ino;
    mkey.mv_size = sizeof(unsigned long long);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            unsigned long long child_ino;
            memcpy(&child_ino, mdata.mv_data,
                   sizeof(unsigned long long));

            /* Skip self-referencing entries */
            if (child_ino == dir_ino)
                goto next_entry;

            filedata = search_dbdata(DBP,
                &child_ino,
                sizeof(unsigned long long),
                NOLOCK);
            if (filedata == NULL)
                goto next_entry;

            ddstat = value_to_ddstat(filedata);
            DATfree(filedata);

            bname = s_basename(ddstat->filename);
            if (bname == NULL
                || strcmp(bname, "/") == 0) {
                if (bname)
                    s_free(bname);
                ddstatfree(ddstat);
                goto next_entry;
            }

            /* Skip entries before 'off' */
            if (idx < off) {
                idx++;
                s_free(bname);
                ddstatfree(ddstat);
                goto next_entry;
            }

            memcpy(&stbuf, &ddstat->stbuf,
                   sizeof(struct stat));
            entsize = fuse_add_direntry(
                req, buf + buf_used,
                bufsize - buf_used,
                bname, &stbuf, idx + 1);

            s_free(bname);

            /*
             * If hardlink (empty filename), get
             * the link name from DBL.
             */
            if (ddstat->filename[0] == 0) {
                DINOINO dinoino;
                MDB_cursor *lcur;
                MDB_val lkey, ldata;
                int lret;

                dinoino.dirnode = dir_ino;
                dinoino.inode =
                    ddstat->stbuf.st_ino;
                lret = mdb_cursor_open(
                    write_txn, dbi_dbl, &lcur);
                if (lret == 0) {
                    lkey.mv_data = &dinoino;
                    lkey.mv_size =
                        sizeof(DINOINO);
                    lret = mdb_cursor_get(
                        lcur, &lkey, &ldata,
                        MDB_SET_KEY);
                    if (lret == 0) {
                        /* Re-add with the right
                           name */
                        char linkname[257];
                        size_t nlen =
                            ldata.mv_size < 256
                            ? ldata.mv_size : 256;
                        memcpy(linkname,
                               ldata.mv_data,
                               nlen);
                        linkname[nlen] = 0;
                        /* Recalculate entsize */
                        buf_used -= entsize;
                        entsize =
                            fuse_add_direntry(
                                req,
                                buf + buf_used,
                                bufsize - buf_used,
                                linkname, &stbuf,
                                idx + 1);
                    }
                    mdb_cursor_close(lcur);
                }
            }
            ddstatfree(ddstat);

            if (entsize > bufsize - buf_used) {
                /* Buffer full */
                break;
            }
            buf_used += entsize;
            idx++;

          next_entry:
            ;
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    release_bdb_lock();
    EFUNC;
    return buf_used;
}

#endif /* LMDB */
