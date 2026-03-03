/*
 * lib_tc_replacements.c - Replacement for Tokyo Cabinet
 *                         utility functions
 *
 * Implements LFTREE (red-black tree), LFLIST (dynamic array),
 * and compression wrappers to eliminate libtokyocabinet.
 *
 * Copyright (c) 2026 - lessfs project
 */

#include "lib_tc_replacements.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <bzlib.h>

/*
 * ======================================
 * LFLIST implementation
 * ======================================
 */

#define LFLIST_INIT_ANUM 64

LFLIST *lflist_new(void)
{
    LFLIST *list = malloc(sizeof(LFLIST));
    if (!list)
        return NULL;
    list->anum = LFLIST_INIT_ANUM;
    list->array = calloc(list->anum,
                         sizeof(LFLISTDATUM));
    list->start = 0;
    list->num = 0;
    return list;
}

void lflist_del(LFLIST *list)
{
    int idx;
    if (!list)
        return;
    for (idx = list->start;
         idx < list->start + list->num; idx++) {
        free(list->array[idx].ptr);
    }
    free(list->array);
    free(list);
}

int lflist_num(const LFLIST *list)
{
    if (!list)
        return 0;
    return list->num;
}

const void *lflist_val(const LFLIST *list,
                       int index, int *sp)
{
    if (!list || index < 0 || index >= list->num)
        return NULL;
    if (sp)
        *sp = list->array[list->start + index].size;
    return list->array[list->start + index].ptr;
}

void lflist_push(LFLIST *list,
                 const void *ptr, int size)
{
    int needed;
    if (!list || !ptr)
        return;
    needed = list->start + list->num + 1;
    if (needed > list->anum) {
        list->anum = needed * 2;
        list->array = realloc(list->array,
            list->anum * sizeof(LFLISTDATUM));
    }
    list->array[list->start + list->num].ptr =
        malloc(size + 1);
    memcpy(
        list->array[list->start + list->num].ptr,
        ptr, size);
    /* Append zero byte like TC does */
    list->array[list->start + list->num]
        .ptr[size] = '\0';
    list->array[list->start + list->num].size =
        size;
    list->num++;
}

/*
 * ======================================
 * Red-black tree (LFTREE) implementation
 * ======================================
 */

/* Per-tree sentinel: each LFTREE has its own
   nil node to avoid data races when multiple
   trees are accessed concurrently under
   different locks. Use TNIL(tree) macro. */
#define TNIL(t) (&(t)->nil)

static void lftree_init_nil(LFTREE *tree)
{
    lfrb_node_t *nil_node = &tree->nil;
    nil_node->key = NULL;
    nil_node->ksiz = 0;
    nil_node->val = NULL;
    nil_node->vsiz = 0;
    nil_node->color = LFRB_BLACK;
    nil_node->left = nil_node;
    nil_node->right = nil_node;
    nil_node->parent = nil_node;
}

static int lfrb_keycmp(const void *aptr, int asiz,
                       const void *bptr, int bsiz)
{
    int minsiz = asiz < bsiz ? asiz : bsiz;
    int cmp = memcmp(aptr, bptr, minsiz);
    if (cmp != 0)
        return cmp;
    if (asiz < bsiz)
        return -1;
    if (asiz > bsiz)
        return 1;
    return 0;
}

static lfrb_node_t *lfrb_node_new(
    const void *kbuf, int ksiz,
    const void *vbuf, int vsiz,
    lfrb_node_t *nil)
{
    lfrb_node_t *node = malloc(sizeof(lfrb_node_t));
    if (!node)
        return NULL;
    node->key = malloc(ksiz + 1);
    memcpy(node->key, kbuf, ksiz);
    node->key[ksiz] = '\0';
    node->ksiz = ksiz;
    node->val = malloc(vsiz + 1);
    memcpy(node->val, vbuf, vsiz);
    node->val[vsiz] = '\0';
    node->vsiz = vsiz;
    node->color = LFRB_RED;
    node->left = nil;
    node->right = nil;
    node->parent = nil;
    return node;
}

static void lfrb_node_free(lfrb_node_t *node)
{
    if (node) {
        free(node->key);
        free(node->val);
        free(node);
    }
}

static void lfrb_rotate_left(LFTREE *tree,
                              lfrb_node_t *node)
{
    lfrb_node_t *right_child = node->right;
    node->right = right_child->left;
    if (right_child->left != TNIL(tree))
        right_child->left->parent = node;
    right_child->parent = node->parent;
    if (node->parent == TNIL(tree))
        tree->root = right_child;
    else if (node == node->parent->left)
        node->parent->left = right_child;
    else
        node->parent->right = right_child;
    right_child->left = node;
    node->parent = right_child;
}

static void lfrb_rotate_right(LFTREE *tree,
                               lfrb_node_t *node)
{
    lfrb_node_t *left_child = node->left;
    node->left = left_child->right;
    if (left_child->right != TNIL(tree))
        left_child->right->parent = node;
    left_child->parent = node->parent;
    if (node->parent == TNIL(tree))
        tree->root = left_child;
    else if (node == node->parent->right)
        node->parent->right = left_child;
    else
        node->parent->left = left_child;
    left_child->right = node;
    node->parent = left_child;
}

static void lfrb_insert_fixup(LFTREE *tree,
                               lfrb_node_t *node)
{
    lfrb_node_t *uncle;
    while (node->parent->color == LFRB_RED) {
        if (node->parent ==
            node->parent->parent->left) {
            uncle = node->parent->parent->right;
            if (uncle->color == LFRB_RED) {
                node->parent->color = LFRB_BLACK;
                uncle->color = LFRB_BLACK;
                node->parent->parent->color =
                    LFRB_RED;
                node = node->parent->parent;
            } else {
                if (node ==
                    node->parent->right) {
                    node = node->parent;
                    lfrb_rotate_left(tree, node);
                }
                node->parent->color = LFRB_BLACK;
                node->parent->parent->color =
                    LFRB_RED;
                lfrb_rotate_right(tree,
                    node->parent->parent);
            }
        } else {
            uncle = node->parent->parent->left;
            if (uncle->color == LFRB_RED) {
                node->parent->color = LFRB_BLACK;
                uncle->color = LFRB_BLACK;
                node->parent->parent->color =
                    LFRB_RED;
                node = node->parent->parent;
            } else {
                if (node ==
                    node->parent->left) {
                    node = node->parent;
                    lfrb_rotate_right(tree, node);
                }
                node->parent->color = LFRB_BLACK;
                node->parent->parent->color =
                    LFRB_RED;
                lfrb_rotate_left(tree,
                    node->parent->parent);
            }
        }
    }
    tree->root->color = LFRB_BLACK;
}

static void lfrb_transplant(LFTREE *tree,
                             lfrb_node_t *old_node,
                             lfrb_node_t *new_node)
{
    if (old_node->parent == TNIL(tree))
        tree->root = new_node;
    else if (old_node == old_node->parent->left)
        old_node->parent->left = new_node;
    else
        old_node->parent->right = new_node;
    new_node->parent = old_node->parent;
}

static lfrb_node_t *lfrb_minimum(
    lfrb_node_t *node,
    lfrb_node_t *nil)
{
    while (node->left != nil)
        node = node->left;
    return node;
}

static void lfrb_delete_fixup(LFTREE *tree,
                               lfrb_node_t *node)
{
    lfrb_node_t *sibling;
    while (node != tree->root &&
           node->color == LFRB_BLACK) {
        if (node == node->parent->left) {
            sibling = node->parent->right;
            if (sibling->color == LFRB_RED) {
                sibling->color = LFRB_BLACK;
                node->parent->color = LFRB_RED;
                lfrb_rotate_left(tree,
                                 node->parent);
                sibling = node->parent->right;
            }
            if (sibling->left->color ==
                    LFRB_BLACK &&
                sibling->right->color ==
                    LFRB_BLACK) {
                sibling->color = LFRB_RED;
                node = node->parent;
            } else {
                if (sibling->right->color ==
                    LFRB_BLACK) {
                    sibling->left->color =
                        LFRB_BLACK;
                    sibling->color = LFRB_RED;
                    lfrb_rotate_right(tree,
                                      sibling);
                    sibling =
                        node->parent->right;
                }
                sibling->color =
                    node->parent->color;
                node->parent->color =
                    LFRB_BLACK;
                sibling->right->color =
                    LFRB_BLACK;
                lfrb_rotate_left(tree,
                                 node->parent);
                node = tree->root;
            }
        } else {
            sibling = node->parent->left;
            if (sibling->color == LFRB_RED) {
                sibling->color = LFRB_BLACK;
                node->parent->color = LFRB_RED;
                lfrb_rotate_right(tree,
                                  node->parent);
                sibling = node->parent->left;
            }
            if (sibling->right->color ==
                    LFRB_BLACK &&
                sibling->left->color ==
                    LFRB_BLACK) {
                sibling->color = LFRB_RED;
                node = node->parent;
            } else {
                if (sibling->left->color ==
                    LFRB_BLACK) {
                    sibling->right->color =
                        LFRB_BLACK;
                    sibling->color = LFRB_RED;
                    lfrb_rotate_left(tree,
                                     sibling);
                    sibling =
                        node->parent->left;
                }
                sibling->color =
                    node->parent->color;
                node->parent->color =
                    LFRB_BLACK;
                sibling->left->color =
                    LFRB_BLACK;
                lfrb_rotate_right(tree,
                                  node->parent);
                node = tree->root;
            }
        }
    }
    node->color = LFRB_BLACK;
}

static lfrb_node_t *lfrb_find(LFTREE *tree,
                               const void *kbuf,
                               int ksiz)
{
    lfrb_node_t *curr = tree->root;
    while (curr != TNIL(tree)) {
        int cmp = lfrb_keycmp(kbuf, ksiz,
                              curr->key,
                              curr->ksiz);
        if (cmp == 0)
            return curr;
        else if (cmp < 0)
            curr = curr->left;
        else
            curr = curr->right;
    }
    return NULL;
}

static void lfrb_free_subtree(
    lfrb_node_t *node,
    lfrb_node_t *nil)
{
    if (node == nil || node == NULL)
        return;
    lfrb_free_subtree(node->left, nil);
    lfrb_free_subtree(node->right, nil);
    lfrb_node_free(node);
}

static void lfrb_collect_keys_inorder(
    lfrb_node_t *node, LFLIST *list,
    lfrb_node_t *nil)
{
    if (node == nil)
        return;
    lfrb_collect_keys_inorder(node->left, list,
                              nil);
    lflist_push(list, node->key, node->ksiz);
    lfrb_collect_keys_inorder(node->right, list,
                              nil);
}

/* In-order successor for iteration */
static lfrb_node_t *lfrb_successor(
    lfrb_node_t *node,
    lfrb_node_t *nil)
{
    lfrb_node_t *parent_node;
    if (node->right != nil)
        return lfrb_minimum(node->right, nil);
    parent_node = node->parent;
    while (parent_node != nil &&
           node == parent_node->right) {
        node = parent_node;
        parent_node = parent_node->parent;
    }
    if (parent_node == nil)
        return NULL;
    return parent_node;
}

/*
 * Public LFTREE API
 */

LFTREE *lftree_new(void)
{
    LFTREE *tree = malloc(sizeof(LFTREE));
    if (!tree)
        return NULL;
    lftree_init_nil(tree);
    tree->root = TNIL(tree);
    tree->cur = NULL;
    tree->rnum = 0;
    return tree;
}

void lftree_del(LFTREE *tree)
{
    if (!tree)
        return;
    lfrb_free_subtree(tree->root, TNIL(tree));
    free(tree);
}

void lftree_clear(LFTREE *tree)
{
    if (!tree)
        return;
    lfrb_free_subtree(tree->root, TNIL(tree));
    lftree_init_nil(tree);
    tree->root = TNIL(tree);
    tree->cur = NULL;
    tree->rnum = 0;
}

void lftree_put(LFTREE *tree, const void *kbuf,
                int ksiz, const void *vbuf,
                int vsiz)
{
    lfrb_node_t *existing;
    lfrb_node_t *node;
    lfrb_node_t *parent_node;
    lfrb_node_t *curr;
    int cmp;

    if (!tree || !kbuf || !vbuf)
        return;

    /* Check if key exists - if so, update */
    existing = lfrb_find(tree, kbuf, ksiz);
    if (existing) {
        free(existing->val);
        existing->val = malloc(vsiz + 1);
        memcpy(existing->val, vbuf, vsiz);
        existing->val[vsiz] = '\0';
        existing->vsiz = vsiz;
        return;
    }

    /* Insert new node */
    node = lfrb_node_new(kbuf, ksiz, vbuf, vsiz,
                         TNIL(tree));
    if (!node)
        return;

    parent_node = TNIL(tree);
    curr = tree->root;
    while (curr != TNIL(tree)) {
        parent_node = curr;
        cmp = lfrb_keycmp(kbuf, ksiz,
                          curr->key, curr->ksiz);
        if (cmp < 0)
            curr = curr->left;
        else
            curr = curr->right;
    }
    node->parent = parent_node;
    if (parent_node == TNIL(tree))
        tree->root = node;
    else if (lfrb_keycmp(kbuf, ksiz,
                         parent_node->key,
                         parent_node->ksiz) < 0)
        parent_node->left = node;
    else
        parent_node->right = node;

    lfrb_insert_fixup(tree, node);
    tree->rnum++;
}

const void *lftree_get(LFTREE *tree,
                       const void *kbuf,
                       int ksiz, int *sp)
{
    lfrb_node_t *node;
    if (!tree || !kbuf)
        return NULL;
    node = lfrb_find(tree, kbuf, ksiz);
    if (!node)
        return NULL;
    if (sp)
        *sp = node->vsiz;
    return node->val;
}

bool lftree_out(LFTREE *tree, const void *kbuf,
                int ksiz)
{
    lfrb_node_t *node;
    lfrb_node_t *fixup_node;
    lfrb_node_t *successor;
    lfrb_color_t orig_color;

    if (!tree || !kbuf)
        return false;
    node = lfrb_find(tree, kbuf, ksiz);
    if (!node)
        return false;

    /* Invalidate iterator if it points here */
    if (tree->cur == node)
        tree->cur = lfrb_successor(node,
                                   TNIL(tree));

    orig_color = node->color;
    if (node->left == TNIL(tree)) {
        fixup_node = node->right;
        lfrb_transplant(tree, node, node->right);
    } else if (node->right == TNIL(tree)) {
        fixup_node = node->left;
        lfrb_transplant(tree, node, node->left);
    } else {
        successor = lfrb_minimum(node->right,
                                 TNIL(tree));
        orig_color = successor->color;
        fixup_node = successor->right;
        if (successor->parent == node) {
            fixup_node->parent = successor;
        } else {
            lfrb_transplant(tree, successor,
                            successor->right);
            successor->right = node->right;
            successor->right->parent = successor;
        }
        lfrb_transplant(tree, node, successor);
        successor->left = node->left;
        successor->left->parent = successor;
        successor->color = node->color;
    }
    lfrb_node_free(node);
    if (orig_color == LFRB_BLACK)
        lfrb_delete_fixup(tree, fixup_node);
    tree->rnum--;
    return true;
}

uint64_t lftree_rnum(const LFTREE *tree)
{
    if (!tree)
        return 0;
    return tree->rnum;
}

LFLIST *lftree_keys(const LFTREE *tree)
{
    LFLIST *list = lflist_new();
    if (!list)
        return NULL;
    lfrb_collect_keys_inorder(tree->root, list,
                              TNIL(tree));
    return list;
}

void lftree_iterinit(LFTREE *tree)
{
    if (!tree)
        return;
    if (tree->root == TNIL(tree)) {
        tree->cur = NULL;
        return;
    }
    tree->cur = lfrb_minimum(tree->root,
                             TNIL(tree));
}

const void *lftree_iternext(LFTREE *tree,
                            int *sp)
{
    lfrb_node_t *node;
    if (!tree || !tree->cur)
        return NULL;
    node = tree->cur;
    tree->cur = lfrb_successor(node,
                               TNIL(tree));
    if (sp)
        *sp = node->ksiz;
    return node->key;
}

const char *lftree_iternext2(LFTREE *tree)
{
    int sp;
    return (const char *)
        lftree_iternext(tree, &sp);
}

/*
 * ======================================
 * Compression wrappers
 * ======================================
 */

char *lf_deflate(const char *ptr, int size,
                 int *sp)
{
    uLongf destlen;
    char *dest;
    int ret;

    if (!ptr || size < 0)
        return NULL;
    destlen = compressBound((uLong) size);
    dest = malloc(destlen + 1);
    if (!dest)
        return NULL;
    ret = compress2((Bytef *) dest, &destlen,
                    (const Bytef *) ptr,
                    (uLong) size,
                    Z_DEFAULT_COMPRESSION);
    if (ret != Z_OK) {
        free(dest);
        return NULL;
    }
    dest[destlen] = '\0';
    if (sp)
        *sp = (int) destlen;
    return dest;
}

char *lf_inflate(const char *ptr, int size,
                 int *sp)
{
    z_stream strm;
    char *dest = NULL;
    int dest_alloc;
    int ret;

    if (!ptr || size < 0)
        return NULL;

    memset(&strm, 0, sizeof(strm));
    strm.next_in = (Bytef *) ptr;
    strm.avail_in = (uInt) size;

    ret = inflateInit(&strm);
    if (ret != Z_OK)
        return NULL;

    dest_alloc = size * 4 + 256;
    dest = malloc(dest_alloc + 1);
    if (!dest) {
        inflateEnd(&strm);
        return NULL;
    }

    strm.next_out = (Bytef *) dest;
    strm.avail_out = dest_alloc;

    while (1) {
        ret = inflate(&strm, Z_NO_FLUSH);
        if (ret == Z_STREAM_END)
            break;
        if (ret != Z_OK && ret != Z_BUF_ERROR) {
            inflateEnd(&strm);
            free(dest);
            return NULL;
        }
        if (strm.avail_out == 0) {
            int old_alloc = dest_alloc;
            dest_alloc *= 2;
            dest = realloc(dest, dest_alloc + 1);
            if (!dest) {
                inflateEnd(&strm);
                return NULL;
            }
            strm.next_out =
                (Bytef *) dest + old_alloc;
            strm.avail_out =
                dest_alloc - old_alloc;
        }
    }
    if (sp)
        *sp = (int) strm.total_out;
    dest[strm.total_out] = '\0';
    inflateEnd(&strm);
    return dest;
}

char *lf_gzip_encode(const char *ptr, int size,
                     int *sp)
{
    z_stream strm;
    char *dest;
    int dest_alloc;
    int ret;

    if (!ptr || size < 0)
        return NULL;

    memset(&strm, 0, sizeof(strm));
    /* windowBits=15+16 for gzip format */
    ret = deflateInit2(&strm,
                       Z_DEFAULT_COMPRESSION,
                       Z_DEFLATED, 15 + 16, 8,
                       Z_DEFAULT_STRATEGY);
    if (ret != Z_OK)
        return NULL;

    dest_alloc = deflateBound(&strm, size);
    dest = malloc(dest_alloc + 1);
    if (!dest) {
        deflateEnd(&strm);
        return NULL;
    }

    strm.next_in = (Bytef *) ptr;
    strm.avail_in = (uInt) size;
    strm.next_out = (Bytef *) dest;
    strm.avail_out = dest_alloc;

    ret = deflate(&strm, Z_FINISH);
    deflateEnd(&strm);
    if (ret != Z_STREAM_END) {
        free(dest);
        return NULL;
    }
    dest[strm.total_out] = '\0';
    if (sp)
        *sp = (int) strm.total_out;
    return dest;
}

char *lf_gzip_decode(const char *ptr, int size,
                     int *sp)
{
    z_stream strm;
    char *dest = NULL;
    int dest_alloc;
    int ret;

    if (!ptr || size < 0)
        return NULL;

    memset(&strm, 0, sizeof(strm));
    /* windowBits=15+16 for gzip auto-detect */
    ret = inflateInit2(&strm, 15 + 16);
    if (ret != Z_OK)
        return NULL;

    dest_alloc = size * 4 + 256;
    dest = malloc(dest_alloc + 1);
    if (!dest) {
        inflateEnd(&strm);
        return NULL;
    }

    strm.next_in = (Bytef *) ptr;
    strm.avail_in = (uInt) size;
    strm.next_out = (Bytef *) dest;
    strm.avail_out = dest_alloc;

    while (1) {
        ret = inflate(&strm, Z_NO_FLUSH);
        if (ret == Z_STREAM_END)
            break;
        if (ret != Z_OK && ret != Z_BUF_ERROR) {
            inflateEnd(&strm);
            free(dest);
            return NULL;
        }
        if (strm.avail_out == 0) {
            int old_alloc = dest_alloc;
            dest_alloc *= 2;
            dest = realloc(dest, dest_alloc + 1);
            if (!dest) {
                inflateEnd(&strm);
                return NULL;
            }
            strm.next_out =
                (Bytef *) dest + old_alloc;
            strm.avail_out =
                dest_alloc - old_alloc;
        }
    }
    if (sp)
        *sp = (int) strm.total_out;
    dest[strm.total_out] = '\0';
    inflateEnd(&strm);
    return dest;
}

char *lf_bzip_encode(const char *ptr, int size,
                     int *sp)
{
    unsigned int destlen;
    char *dest;
    int ret;

    if (!ptr || size < 0)
        return NULL;
    destlen = (unsigned int)
        (size + size / 100 + 600 + 1);
    dest = malloc(destlen + 1);
    if (!dest)
        return NULL;
    ret = BZ2_bzBuffToBuffCompress(
        dest, &destlen, (char *) ptr,
        (unsigned int) size, 9, 0, 30);
    if (ret != BZ_OK) {
        free(dest);
        return NULL;
    }
    dest[destlen] = '\0';
    if (sp)
        *sp = (int) destlen;
    return dest;
}

char *lf_bzip_decode(const char *ptr, int size,
                     int *sp)
{
    unsigned int destlen;
    char *dest;
    int ret;

    if (!ptr || size < 0)
        return NULL;
    /* Start with 4x, retry with larger */
    destlen = (unsigned int)(size * 4 + 256);
    dest = malloc(destlen + 1);
    if (!dest)
        return NULL;

    while (1) {
        ret = BZ2_bzBuffToBuffDecompress(
            dest, &destlen,
            (char *) ptr,
            (unsigned int) size, 0, 0);
        if (ret == BZ_OK)
            break;
        if (ret == BZ_OUTBUFF_FULL) {
            destlen *= 2;
            dest = realloc(dest, destlen + 1);
            if (!dest)
                return NULL;
            continue;
        }
        free(dest);
        return NULL;
    }
    dest[destlen] = '\0';
    if (sp)
        *sp = (int) destlen;
    return dest;
}
