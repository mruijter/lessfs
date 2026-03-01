/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   This program is free software;  you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY;  without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
 *   the GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program;  if not, write to the Free Software
 *   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>
#include <fuse.h>

#include <tcutil.h>
#include <stdlib.h>
#include <stdbool.h>

#include "lib_log.h"
#include "lib_safe.h"
#include "lib_cfg.h"
#include "lib_str.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_common.h"
#ifdef BERKELEYDB
#include <db.h>
#include "lib_bdb.h"
#elif defined(LMDB)
#include <lmdb.h>
#include "lib_lmdb.h"
#endif
#include "lib_repl.h"
#include "file_io.h"

#ifdef ENABLE_CRYPTO
unsigned char *passwd = NULL;
#endif

#define die_dataerr(f...) { LFATAL(f); exit(EXIT_DATAERR); }
#define die_syserr() { LFATAL("Fatal system error : %s",strerror(errno)); exit(EXIT_SYSTEM); }

#include "commons.h"
#define BACKSPACE 8

unsigned long long lafinode;
extern unsigned long long nextoffset;
unsigned long long detected_size = 0;
extern int fdbdta;
unsigned long working;

struct option_info {
    char *configfile;
    int optimizetc;
    int fast;
    int thorough;
};
struct option_info chkoptions;

unsigned long long check_inuse(unsigned char *lfshash)
{
    unsigned long long counter;
    DAT *data;

    if (NULL == lfshash)
        return (0);

    data = search_dbdata(DBU, lfshash, config->hashlen, LOCK);
    if (NULL == data) {
        LDEBUG("check_inuse nothing found return 0.");
        return (0);
    }
    memcpy(&counter, data->data, sizeof(counter));
    DATfree(data);
    return counter;
}

void show_progress()
{
    static int progress = 0;

    progress++;
    if (progress == 100)
        printf("%c|", BACKSPACE);
    if (progress == 200)
        printf("%c/", BACKSPACE);
    if (progress == 300)
        printf("%c-", BACKSPACE);
    if (progress == 400)
        printf("%c\\", BACKSPACE);
    if (progress == 500) {
        progress = 0;
    }
}

void printhash(char *msg, unsigned char *bhash)
{
    char *ascii_hash = NULL;
    int n;
    char *p1, *p2;

    for (n = 0; n < config->hashlen; n++) {
        p1 = as_sprintf(__FILE__, __LINE__, "%02X", bhash[n]);
        if (n == 0) {
            ascii_hash = s_strdup(p1);
        } else {
            p2 = s_strdup(ascii_hash);
            free(ascii_hash);
            ascii_hash = as_sprintf(__FILE__, __LINE__, "%s%s", p2, p1);
            free(p2);
        }
        free(p1);
    }
    printf("%s : %s\n", msg, ascii_hash);
    free(ascii_hash);
}

void usage(char *name)
{
    printf("Usage   : %s [-o] [-f] [-t] -c /path_to_config.cfg\n", name);
    printf("        : -o Optimize the databases.\n");
    printf
        ("        :    This operation requires enough free space to contain a full copy of the database!\n");
    printf
        ("        :    Optimizing the database is advised after a crash but often we can do without.\n");
    printf("        :\n");
    printf
        ("        : -f Start fsck without delay, lessfs is not mounted.\n");
    printf
        ("        : -t Thorough mode inspects not only the metadata but the file structures as well.\n");
    printf("Version : %s\n", VERSION);
    exit(-1);
}

int get_opts(int argc, char *argv[])
{

    int c;

    chkoptions.optimizetc = 0;
    chkoptions.fast = 0;
    chkoptions.thorough = 0;
    chkoptions.configfile = NULL;
    while ((c = getopt(argc, argv, "tfoc:")) != -1)
        switch (c) {
        case 'o':
            chkoptions.optimizetc = 1;
            break;
        case 'c':
            if (optopt == 'c')
                printf
                    ("Option -%c requires a lessfs configuration file as argument.\n",
                     optopt);
            else
                chkoptions.configfile = optarg;
            break;
        case 'f':
            chkoptions.fast = 1;
            break;
        case 't':
            chkoptions.thorough = 1;
            break;
        default:
            abort();
        }
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc < 2)
        usage(argv[0]);
    fprintf(stderr,
            "lessfsck is not currently supported with berkeleydb\n");
    exit(EXIT_USAGE);
}
