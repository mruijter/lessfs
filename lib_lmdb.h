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
#ifndef LIB_LMDB_H
#define LIB_LMDB_H

#include <lmdb.h>

#define LMDB_DEFAULT_MAPSIZE (10ULL * 1024 * 1024 * 1024)
#define LMDB_ERRORLOG "/var/log/lessfs-lmdb_err.txt"

#ifndef die_dataerr
#define die_dataerr(f...) \
    { LFATAL(f); exit(EXIT_DATAERR); }
#endif
#ifndef die_syserr
#define die_syserr() \
    { LFATAL("Fatal system error : %s", \
      strerror(errno)); exit(EXIT_SYSTEM); }
#endif
void bdb_close(void);
void bdb_checkpoint(void);
void start_transactions(void);
void commit_transactions(void);
void bdb_open(void);
DAT *search_dbdata(int, void *, int, bool);
void drop_databases(void);
void delete_key(int, void *, int, const char *);
void bin_write_dbdata(int, void *, int, void *, int);
void btbin_write_dup(int, void *, int, void *, int,
                     bool);
void btbin_write_dbdata(int, void *, int, void *, int);
void start_flush_commit(void);
int bt_entry_exists(int, void *, int, void *, int);
DDSTAT *dnode_bname_to_inode(void *, int, char *);
DAT *btsearch_keyval(int, void *, int, void *, int,
                     bool);
int count_dirlinks(void *, int);
int fs_readdir(const char *, void *, fuse_fill_dir_t,
               off_t, struct fuse_file_info *);
void fs_read_hardlink(struct stat, DDSTAT *, void *,
                      fuse_fill_dir_t,
                      struct fuse_file_info *);
void bdb_restart_truncation(void);
unsigned long long get_offset_fast(unsigned long long);
INUSE *get_offset_reclaim(unsigned long long,
                          unsigned long long);
unsigned long long has_nodes(unsigned long long);
void bin_write(int, void *, int, void *, int);
int btdelete_curkey(int, void *, int, void *, int,
                    const char *);
void release_bdb_lock(void);
int try_bdb_lock(void);
void bdb_lock(const char *);
void end_flush_commit(void);
void listdbp(void);
void listdbb(void);
void listdbu(void);
void listdta(void);
void list_symlinks(void);
void flistdbu(void);
void listfree(int freespace_summary);
void listdirent(void);
void list_hardlinks(void);
void bdb_stat(void);
void abort_transactions(void);

#endif /* LIB_LMDB_H */
