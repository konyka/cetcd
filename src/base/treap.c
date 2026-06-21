#include "cetcd/base.h"

#include <stdlib.h>
#include <string.h>

#include "cetcd/slice.h"
#include "cetcd/treap.h"

/* Internal treap implementation details. */
typedef struct cetcd_treap_node {
    cetcd_slice key;
    void *value;
    uint32_t priority;
    struct cetcd_treap_node *left;
    struct cetcd_treap_node *right;
} cetcd_treap_node;

struct cetcd_treap {
    cetcd_treap_node *root;
    size_t size;
};

/* Helpers: simple deterministic hash-based priority generation from the key. */
static uint32_t treap_prio_from_key_(cetcd_slice key) {
    /* Use a tiny FNV-like mix over the bytes to obtain a reasonably random
     * distribution while remaining deterministic for a given key. */
    uint32_t h = 0x811c9dc5u; /* FNV offset basis */
    for (size_t i = 0; i < key.len; ++i) {
        h ^= (uint32_t)key.data[i];
        h *= 0x01000193u; /* FNV prime */
    }
    /* Mix with the length to reduce collisions for identical prefixes. */
    h ^= (uint32_t)key.len;
    return h;
}

/* Rotations */
static cetcd_treap_node *treap_rotate_right_(cetcd_treap_node *y) {
    cetcd_treap_node *x = y->left;
    cetcd_treap_node *T2 = x->right;

    x->right = y;
    y->left = T2;
    return x;
}

static cetcd_treap_node *treap_rotate_left_(cetcd_treap_node *x) {
    cetcd_treap_node *y = x->right;
    cetcd_treap_node *T2 = y->left;

    y->left = x;
    x->right = T2;
    return y;
}

/* Find a node by key. Returns pointer and sets found flag. */
static cetcd_treap_node *treap_find_(cetcd_treap_node *root, cetcd_slice key, bool *found_out) {
    cetcd_treap_node *p = root;
    while (p != NULL) {
        int cmp = cetcd_slice_compare(key, p->key);
        if (cmp == 0) {
            if (found_out) *found_out = true;
            return p;
        } else if (cmp < 0) {
            p = p->left;
        } else {
            p = p->right;
        }
    }
    if (found_out) *found_out = false;
    return NULL;
}

/* BST insert of a new node. Caller ensures key does not exist. */
static cetcd_treap_node *treap_insert_node_(cetcd_treap_node *root, cetcd_treap_node *n) {
    if (root == NULL) return n;
    int cmp = cetcd_slice_compare(n->key, root->key);
    if (cmp < 0) {
        root->left = treap_insert_node_(root->left, n);
        if (root->left && root->left->priority > root->priority) {
            root = treap_rotate_right_(root);
        }
    } else { /* cmp > 0 */
        root->right = treap_insert_node_(root->right, n);
        if (root->right && root->right->priority > root->priority) {
            root = treap_rotate_left_(root);
        }
    }
    return root;
}

/* Delete by key. Returns new root; out_found informs if deletion happened. */
static cetcd_treap_node *treap_delete_node_(cetcd_treap_node *root,
                                          cetcd_slice key,
                                          bool *out_found,
                                          void **val_out) {
    if (root == NULL) {
        if (out_found) *out_found = false;
        return NULL;
    }
    int cmp = cetcd_slice_compare(key, root->key);
    if (cmp < 0) {
        root->left = treap_delete_node_(root->left, key, out_found, val_out);
        return root;
    } else if (cmp > 0) {
        root->right = treap_delete_node_(root->right, key, out_found, val_out);
        return root;
    } else {
        /* Found. Remove this node. */
        if (out_found) *out_found = true;
        if (val_out) *val_out = root->value;

        /* If leaf, free and return NULL */
        if (root->left == NULL && root->right == NULL) {
            if (root->key.data) free((void*)root->key.data);
            free(root);
            return NULL;
        }
        /* If two children, rotate with higher-priority child to push node down. */
        if (root->left != NULL && root->right != NULL) {
            if (root->left->priority > root->right->priority) {
                root = treap_rotate_right_(root);
                root->right = treap_delete_node_(root->right, key, out_found, val_out);
                return root;
            } else {
                root = treap_rotate_left_(root);
                root->left = treap_delete_node_(root->left, key, out_found, val_out);
                return root;
            }
        }
        /* Only one child: replace node with that child. */
        cetcd_treap_node *child = (root->left != NULL) ? root->left : root->right;
        if (root->key.data) free((void*)root->key.data);
        free(root);
        return child;
    }
}

/* Free a subtree. */
static void treap_free_subtree_(cetcd_treap_node *n) {
    if (n == NULL) return;
    treap_free_subtree_(n->left);
    treap_free_subtree_(n->right);
    if (n->key.data) free((void*)n->key.data);
    free(n);
}

/* Helper for range query: in-order traversal with range filtering. */
static bool treap_inorder_range_(cetcd_treap_node *n, cetcd_slice lo, cetcd_slice hi, cetcd_treap_iter_fn fn, void *udata) {
    if (n == NULL) return true;
    if (!treap_inorder_range_(n->left, lo, hi, fn, udata)) return false;
    if (cetcd_slice_compare(n->key, lo) >= 0 && cetcd_slice_compare(n->key, hi) < 0) {
        if (!fn(n->key, n->value, udata)) return false;
    }
    if (!treap_inorder_range_(n->right, lo, hi, fn, udata)) return false;
    return true;
}

/* Public API */
cetcd_treap *cetcd_treap_new(void) {
    cetcd_treap *t = (cetcd_treap *)calloc(1, sizeof(*t));
    if (t == NULL) return NULL;
    t->root = NULL;
    t->size = 0;
    return t;
}


void cetcd_treap_free(cetcd_treap *t) {
    if (t == NULL) return;
    treap_free_subtree_(t->root);
    free(t);
}

size_t cetcd_treap_size(const cetcd_treap *t) {
    return t ? t->size : 0;
}

int cetcd_treap_put(cetcd_treap *t, cetcd_slice key, void *value) {
    if (t == NULL) return CETCD_ERR_INVAL;

    /* Check if key exists first. */
    bool found = false;
    cetcd_treap_node *p = t->root;
    cetcd_treap_node *parent = NULL;
    while (p != NULL) {
        int cmp = cetcd_slice_compare(key, p->key);
        if (cmp == 0) {
            /* Replace value for existing key. */
            p->value = value;
            return CETCD_OK;
        } else if (cmp < 0) {
            parent = p;
            p = p->left;
        } else {
            parent = p;
            p = p->right;
        }
    }

    /* Not found: create a new node. */
    cetcd_treap_node *n = (cetcd_treap_node *)calloc(1, sizeof(*n));
    if (n == NULL) return CETCD_ERR_NOMEM;
    /* Copy key. */
    if (key.len > 0) {
        n->key.data = (const uint8_t *)malloc(key.len);
        if (n->key.data == NULL) { free(n); return CETCD_ERR_NOMEM; }
        memcpy((void *)n->key.data, key.data, key.len);
        n->key.len = key.len;
    } else {
        n->key.data = NULL;
        n->key.len = 0;
    }
    n->value = value;
    n->left = NULL;
    n->right = NULL;
    n->priority = treap_prio_from_key_(key);

    /* BST insert */
    t->root = treap_insert_node_(t->root, n);
    t->size += 1;
    return CETCD_OK;
}

bool cetcd_treap_get(const cetcd_treap *t, cetcd_slice key, void **val_out) {
    if (t == NULL) return false;
    cetcd_treap_node *p = t->root;
    while (p != NULL) {
        int cmp = cetcd_slice_compare(key, p->key);
        if (cmp == 0) {
            if (val_out) *val_out = p->value;
            return true;
        } else if (cmp < 0) {
            p = p->left;
        } else {
            p = p->right;
        }
    }
    return false;
}

bool cetcd_treap_del(cetcd_treap *t, cetcd_slice key, void **val_out) {
    if (t == NULL || t->root == NULL) return false;
    bool found = false;
    t->root = treap_delete_node_(t->root, key, &found, val_out);
    if (found) {
        if (t->size > 0) --t->size;
        return true;
    }
    return false;
}

static bool treap_iter_inorder_(cetcd_treap_node *n, cetcd_treap_iter_fn fn, void *udata) {
    if (n == NULL) return true;
    if (!treap_iter_inorder_(n->left, fn, udata)) return false;
    if (!fn(n->key, n->value, udata)) return false;
    if (!treap_iter_inorder_(n->right, fn, udata)) return false;
    return true;
}

void cetcd_treap_iter(const cetcd_treap *t, cetcd_treap_iter_fn fn, void *udata) {
    if (t == NULL || fn == NULL) return;
    treap_iter_inorder_(t->root, fn, udata);
}

void cetcd_treap_range(const cetcd_treap *t, cetcd_slice lo, cetcd_slice hi, cetcd_treap_iter_fn fn, void *udata) {
    if (t == NULL || fn == NULL) return;
    /* In-order traversal with range filtering using a helper that is defined
     * above. */
    treap_inorder_range_(t->root, lo, hi, fn, udata);
}
