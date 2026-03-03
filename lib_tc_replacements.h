/*
 * lib_tc_replacements.h - Replacement for Tokyo Cabinet utility functions
 *
 * Provides LFTREE (ordered key-value tree), LFLIST (dynamic array),
 * and compression wrappers (gzip, bzip2, deflate) to eliminate the
 * dependency on libtokyocabinet.
 *
 * Copyright (c) 2026 - lessfs project
 */

#ifndef LIB_TC_REPLACEMENTS_H
#define LIB_TC_REPLACEMENTS_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/*
 * ======================================
 * LFLIST - Dynamic array (replaces TCLIST)
 * ======================================
 */

typedef struct {
    char *ptr;
    int size;
} LFLISTDATUM;

typedef struct {
    LFLISTDATUM *array;
    int anum;    /* allocated number of elements */
    int start;   /* start index of used elements */
    int num;     /* number of used elements */
} LFLIST;

LFLIST *lflist_new(void);
void lflist_del(LFLIST *list);
int lflist_num(const LFLIST *list);
const void *lflist_val(const LFLIST *list,
                       int index, int *sp);
void lflist_push(LFLIST *list,
                 const void *ptr, int size);

/*
 * ======================================
 * LFTREE - Red-black tree
 *          (replaces TCTREE)
 * ======================================
 */

typedef enum { LFRB_RED, LFRB_BLACK } lfrb_color_t;

typedef struct lfrb_node {
    char *key;
    int ksiz;
    char *val;
    int vsiz;
    lfrb_color_t color;
    struct lfrb_node *left;
    struct lfrb_node *right;
    struct lfrb_node *parent;
} lfrb_node_t;

typedef struct {
    lfrb_node_t nil;    /* per-tree sentinel */
    lfrb_node_t *root;
    lfrb_node_t *cur;   /* iterator cursor */
    uint64_t rnum;      /* number of records */
} LFTREE;

LFTREE *lftree_new(void);
void lftree_del(LFTREE *tree);
void lftree_clear(LFTREE *tree);
void lftree_put(LFTREE *tree, const void *kbuf,
                int ksiz, const void *vbuf,
                int vsiz);
const void *lftree_get(LFTREE *tree,
                       const void *kbuf,
                       int ksiz, int *sp);
bool lftree_out(LFTREE *tree, const void *kbuf,
                int ksiz);
uint64_t lftree_rnum(const LFTREE *tree);
LFLIST *lftree_keys(const LFTREE *tree);
void lftree_iterinit(LFTREE *tree);
const void *lftree_iternext(LFTREE *tree,
                            int *sp);
const char *lftree_iternext2(LFTREE *tree);

/*
 * ======================================
 * Compression wrappers
 * (replace tcgzip*, tcbzip*, tcdeflate,
 *  tcinflate)
 * ======================================
 */

char *lf_deflate(const char *ptr, int size,
                 int *sp);
char *lf_inflate(const char *ptr, int size,
                 int *sp);
char *lf_gzip_encode(const char *ptr, int size,
                     int *sp);
char *lf_gzip_decode(const char *ptr, int size,
                     int *sp);
char *lf_bzip_encode(const char *ptr, int size,
                     int *sp);
char *lf_bzip_decode(const char *ptr, int size,
                     int *sp);

/*
 * ======================================
 * Compatibility macros - map old TC names
 * to new LF names so existing code
 * compiles without changes
 * ======================================
 */

/* Types */
#define TCTREE      LFTREE
#define TCLIST      LFLIST
#define TCLISTDATUM LFLISTDATUM

/* TCTREE functions */
#define tctreenew    lftree_new
#define tctreedel    lftree_del
#define tctreeclear  lftree_clear
#define tctreeput    lftree_put
#define tctreeget    lftree_get
#define tctreeout    lftree_out
#define tctreernum   lftree_rnum
#define tctreekeys   lftree_keys
#define tctreeiterinit   lftree_iterinit
#define tctreeiternext   lftree_iternext
#define tctreeiternext2  lftree_iternext2

/* TCLIST functions */
#define tclistnew    lflist_new
#define tclistdel    lflist_del
#define tclistnum    lflist_num
#define tclistval    lflist_val
#define tclistpush   lflist_push

/* Compression functions */
#define tcdeflate    lf_deflate
#define tcinflate    lf_inflate
#define tcgzipencode lf_gzip_encode
#define tcgzipdecode lf_gzip_decode
#define tcbzipencode lf_bzip_encode
#define tcbzipdecode lf_bzip_decode

#endif /* LIB_TC_REPLACEMENTS_H */
